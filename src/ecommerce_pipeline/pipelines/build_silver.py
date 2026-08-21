from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_commit_metadata,
    drop_dangling_catalog_registration,
    latest_delta_pipeline_commit,
    read_delta,
    try_delta_table_state,
    write_delta,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_SCHEMA_VERSION
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES, SilverTableContract, get_silver_contract
from ecommerce_pipeline.control.manifests import (
    BronzeBatchManifest,
    SilverBatchManifest,
    SilverTableResult,
)
from ecommerce_pipeline.transformations.silver.common import (
    SILVER_SCHEMA_VERSION,
    SILVER_SEQUENCE_COLUMNS,
    silver_change_history_table_name,
)
from ecommerce_pipeline.transformations.silver.customers import (
    supports_customer_table,
    transform_customer_history,
    transform_customer_table,
)
from ecommerce_pipeline.transformations.silver.sales import (
    supports_sales_table,
    transform_sales_history,
    transform_sales_table,
)

SILVER_PIPELINE_NAME = "bronze_to_silver"
SILVER_CHANGE_HISTORY_PIPELINE_NAME = "bronze_to_silver_change_history"
SILVER_DATA_PIPELINES = (SILVER_PIPELINE_NAME, "cdc_to_silver")


class SilverBuilder:
    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        bronze_manifest: BronzeBatchManifest | None = None,
        timings_ms: dict[str, int] | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)
        self.bronze_manifest = bronze_manifest
        self.timings_ms = timings_ms

    def run(
        self,
        table_names: list[str] | None = None,
        *,
        batch_id: str,
        full_rebuild: bool = False,
    ) -> SilverBatchManifest:
        tables = list(SILVER_TABLES) if table_names is None else table_names
        unknown = sorted(set(tables) - set(SILVER_TABLES))
        if unknown:
            raise ValueError(f"Unknown silver source tables: {unknown}")

        def process(table_name: str) -> SilverTableResult:
            worker = SilverBuilder(
                self.spark.newSession(),
                self.config,
                bronze_manifest=self.bronze_manifest,
                timings_ms=self.timings_ms,
            )
            started = perf_counter()
            try:
                return worker.run_table(table_name, batch_id=batch_id, full_rebuild=full_rebuild)
            finally:
                if self.timings_ms is not None:
                    self.timings_ms[f"silver.table.{table_name}"] = round((perf_counter() - started) * 1000)

        workers = max(1, min(len(tables), self.config.spark.max_parallel_tables))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(process, tables))
        return SilverBatchManifest(tables=dict(zip(tables, results, strict=True)))

    def run_table(self, table_name: str, *, batch_id: str, full_rebuild: bool = False) -> SilverTableResult:
        contract = get_silver_contract(table_name)
        current_bronze_version = self._bronze_version(table_name)
        silver_reference = self.config.lakehouse.table_reference("silver", table_name)
        silver_state = try_delta_table_state(
            self.spark,
            silver_reference,
            pipeline=SILVER_PIPELINE_NAME,
        )

        if full_rebuild or silver_state is None:
            self._replace_from_snapshot(contract, current_bronze_version, batch_id)
            version = 0 if silver_state is None else silver_state.version + 1
            return self._result(
                contract,
                version,
            )

        # CDC may bootstrap Unified Silver before the first Batch execution.
        # Establish Batch progress with a sequence-guarded MERGE; replacing the
        # table here could discard a newer CDC event that arrived after the
        # Batch snapshot was extracted.
        if silver_state.progress is None:
            self._merge_snapshot_into_unified(contract, current_bronze_version, batch_id)
            committed = latest_delta_pipeline_commit(
                self.spark,
                silver_reference,
                pipelines=SILVER_DATA_PIPELINES,
            )
            return self._result(contract, committed.version)

        previous_bronze_version = self._processed_version(table_name, silver_state.progress)
        if previous_bronze_version is None:
            raise RuntimeError(
                f"Silver progress metadata is missing for {table_name}; reset Silver or run with --full-rebuild-silver"
            )
        if previous_bronze_version > current_bronze_version:
            raise RuntimeError(
                f"Silver progress is ahead of Bronze for {table_name}: "
                f"silver_version={previous_bronze_version}, bronze_version={current_bronze_version}"
            )
        if previous_bronze_version == current_bronze_version:
            if silver_state.progress_version is None:
                raise RuntimeError(f"Silver progress version is missing for table: {table_name}")
            return self._result(
                contract,
                silver_state.progress_version,
            )

        self._validate_schema_version(table_name, silver_state.progress)
        starting_version = previous_bronze_version + 1
        changes = self.read_changes(
            table_name,
            starting_version=starting_version,
            ending_version=current_bronze_version,
        )
        owns_changes_cache = contract.materialize_change_history and not changes.is_cached
        shared_changes = changes.cache() if owns_changes_cache else changes
        try:
            if contract.materialize_change_history:
                history_started = perf_counter()
                history = self.transform_history(contract, shared_changes)
                self.append_change_history(
                    contract,
                    history,
                    batch_id=batch_id,
                    bronze_version=current_bronze_version,
                    bronze_starting_version=starting_version,
                    transaction_version=current_bronze_version,
                )
                self._record_timing(f"silver.history.{table_name}", history_started)

            merge_started = perf_counter()
            transformed = self.transform(contract, shared_changes)
            with delta_commit_metadata(
                self.spark,
                self._progress_metadata(
                    table_name,
                    current_bronze_version,
                    batch_id,
                    bronze_starting_version=starting_version,
                ),
            ):
                self.lakehouse.upsert_table(
                    transformed,
                    "silver",
                    table_name,
                    contract.primary_keys,
                    delete_mode="soft",
                    sequence_columns=SILVER_SEQUENCE_COLUMNS,
                    target_exists=True,
                    source_is_nonempty=True,
                )
            self._record_timing(f"silver.merge.{table_name}", merge_started)
        finally:
            if owns_changes_cache:
                shared_changes.unpersist()
        return self._result(
            contract,
            silver_state.version + 1,
        )

    def transform(self, contract: SilverTableContract, bronze_df: DataFrame) -> DataFrame:
        if supports_customer_table(contract.table_name):
            return transform_customer_table(bronze_df, contract)
        if supports_sales_table(contract.table_name):
            return transform_sales_table(bronze_df, contract)
        raise ValueError(f"Missing silver transformation for table: {contract.table_name}")

    def transform_history(self, contract: SilverTableContract, bronze_df: DataFrame) -> DataFrame:
        if supports_customer_table(contract.table_name):
            return transform_customer_history(bronze_df, contract)
        if supports_sales_table(contract.table_name):
            return transform_sales_history(bronze_df, contract)
        raise ValueError(f"Missing silver history transformation for table: {contract.table_name}")

    def read_snapshot(self, table_name: str) -> DataFrame:
        return read_delta(self.spark, self.config.lakehouse.table_reference("bronze", table_name))

    def read_changes(self, table_name: str, *, starting_version: int, ending_version: int) -> DataFrame:
        return (
            read_delta(
                self.spark,
                self.config.lakehouse.table_reference("bronze", table_name),
                options={
                    "readChangeFeed": "true",
                    "startingVersion": starting_version,
                    "endingVersion": ending_version,
                },
            )
            .where("_change_type = 'insert'")
            .drop(
                "_change_type",
                "_commit_version",
                "_commit_timestamp",
            )
        )

    @staticmethod
    def _processed_version(table_name: str, metadata: dict[str, object] | None) -> int | None:
        if metadata is None:
            return None
        value = metadata.get("last_processed_bronze_version")
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"Invalid Silver Delta progress for table: {table_name}")
        return value

    def _replace_from_snapshot(
        self,
        contract: SilverTableContract,
        bronze_version: int,
        batch_id: str,
    ) -> None:
        source = self.read_snapshot(contract.table_name)
        owns_source_cache = contract.materialize_change_history and not source.is_cached
        shared_source = source.cache() if owns_source_cache else source
        try:
            snapshot_started = perf_counter()
            snapshot = self.transform(contract, shared_source)
            with delta_commit_metadata(
                self.spark,
                self._progress_metadata(
                    contract.table_name,
                    bronze_version,
                    batch_id,
                    bronze_starting_version=None,
                ),
            ):
                self.lakehouse.write_table(
                    snapshot,
                    "silver",
                    contract.table_name,
                    enable_change_data_feed=True,
                )
            self._record_timing(f"silver.snapshot.{contract.table_name}", snapshot_started)
            if not contract.materialize_change_history:
                return
            history_started = perf_counter()
            history = self.transform_history(contract, shared_source)
            with delta_commit_metadata(
                self.spark,
                self._history_progress_metadata(
                    contract.table_name,
                    bronze_version,
                    batch_id,
                    bronze_starting_version=None,
                ),
            ):
                self.lakehouse.write_table(
                    history,
                    "silver",
                    silver_change_history_table_name(contract.table_name),
                    enable_change_data_feed=True,
                )
            self._record_timing(f"silver.history.{contract.table_name}", history_started)
        finally:
            if owns_source_cache:
                shared_source.unpersist()

    def _merge_snapshot_into_unified(
        self,
        contract: SilverTableContract,
        bronze_version: int,
        batch_id: str,
    ) -> None:
        """Initialize Batch progress without replacing CDC-owned Silver state."""

        source = self.read_snapshot(contract.table_name)
        owns_source_cache = contract.materialize_change_history and not source.is_cached
        shared_source = source.cache() if owns_source_cache else source
        try:
            if contract.materialize_change_history:
                history_started = perf_counter()
                self.append_change_history(
                    contract,
                    self.transform_history(contract, shared_source),
                    batch_id=batch_id,
                    bronze_version=bronze_version,
                    bronze_starting_version=None,
                    transaction_version=bronze_version,
                )
                self._record_timing(f"silver.history.{contract.table_name}", history_started)

            merge_started = perf_counter()
            snapshot = self.transform(contract, shared_source)
            with delta_commit_metadata(
                self.spark,
                self._progress_metadata(
                    contract.table_name,
                    bronze_version,
                    batch_id,
                    bronze_starting_version=None,
                ),
            ):
                self.lakehouse.upsert_table(
                    snapshot,
                    "silver",
                    contract.table_name,
                    contract.primary_keys,
                    delete_mode="soft",
                    sequence_columns=SILVER_SEQUENCE_COLUMNS,
                    target_exists=True,
                    source_is_nonempty=True,
                )
            self._record_timing(f"silver.merge.{contract.table_name}", merge_started)
        finally:
            if owns_source_cache:
                shared_source.unpersist()

    def append_change_history(
        self,
        contract: SilverTableContract,
        history: DataFrame,
        *,
        batch_id: str,
        bronze_version: int,
        bronze_starting_version: int | None,
        transaction_version: int,
        pipeline_name: str = SILVER_CHANGE_HISTORY_PIPELINE_NAME,
        transaction_app_id: str | None = None,
    ) -> bool:
        """Append immutable Silver Change History rows with Delta retry idempotency."""

        table_name = silver_change_history_table_name(contract.table_name)
        reference = self.config.lakehouse.table_reference("silver", table_name)
        if reference.is_catalog:
            drop_dangling_catalog_registration(self.spark, reference)
        metadata = self._history_progress_metadata(
            contract.table_name,
            bronze_version,
            batch_id,
            bronze_starting_version=bronze_starting_version,
            pipeline_name=pipeline_name,
        )
        application_id = transaction_app_id or (f"{pipeline_name}:{contract.table_name}:v{SILVER_SCHEMA_VERSION}")
        writer = (
            history.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .option("txnAppId", application_id)
            .option("txnVersion", transaction_version)
            .option("userMetadata", json.dumps(metadata, separators=(",", ":"), sort_keys=True))
        )
        write_delta(writer, reference)
        return True

    @staticmethod
    def _progress_metadata(
        table_name: str,
        bronze_version: int,
        batch_id: str,
        *,
        bronze_starting_version: int | None,
    ) -> dict[str, object]:
        return {
            "pipeline": SILVER_PIPELINE_NAME,
            "source_table": table_name,
            "batch_id": batch_id,
            "last_processed_bronze_version": bronze_version,
            "bronze_starting_version": bronze_starting_version,
            "silver_schema_version": SILVER_SCHEMA_VERSION,
        }

    @staticmethod
    def _history_progress_metadata(
        table_name: str,
        bronze_version: int,
        batch_id: str,
        *,
        bronze_starting_version: int | None,
        pipeline_name: str = SILVER_CHANGE_HISTORY_PIPELINE_NAME,
    ) -> dict[str, object]:
        return {
            "pipeline": pipeline_name,
            "source_table": table_name,
            "history_table": silver_change_history_table_name(table_name),
            "batch_id": batch_id,
            "last_processed_bronze_version": bronze_version,
            "bronze_starting_version": bronze_starting_version,
            "silver_schema_version": SILVER_SCHEMA_VERSION,
            "append_only": True,
        }

    def _validate_schema_version(self, table_name: str, metadata: dict[str, object] | None) -> None:
        version = None if metadata is None else metadata.get("silver_schema_version")
        if version != SILVER_SCHEMA_VERSION:
            raise RuntimeError(f"Silver schema version is outdated for {table_name}; run with --full-rebuild-silver")

    def _bronze_version(self, table_name: str) -> int:
        if self.bronze_manifest is not None:
            try:
                result = self.bronze_manifest.tables[table_name]
            except KeyError as exc:
                raise ValueError(f"Bronze version was not provided for table: {table_name}") from exc
            if result.schema_version != BRONZE_SCHEMA_VERSION:
                raise RuntimeError(f"Bronze schema version is outdated for {table_name}: {result.schema_version}")
            return result.delta_version
        state = try_delta_table_state(
            self.spark,
            self.config.lakehouse.table_reference("bronze", table_name),
            pipeline="postgres_to_bronze",
        )
        if state is None:
            raise RuntimeError(f"Bronze table is missing: {table_name}")
        if state.progress_version is None:
            raise RuntimeError(f"Bronze progress metadata is missing for table: {table_name}")
        return state.progress_version

    def _result(
        self,
        contract: SilverTableContract,
        committed_version: int,
    ) -> SilverTableResult:
        return SilverTableResult(
            table_name=contract.table_name,
            committed_version=committed_version,
            schema_version=SILVER_SCHEMA_VERSION,
        )

    def _record_timing(self, name: str, started: float) -> None:
        if self.timings_ms is not None:
            self.timings_ms[name] = round((perf_counter() - started) * 1000)


def build_silver(
    spark: SparkSession,
    config: AppConfig,
    table_names: list[str] | None = None,
    *,
    batch_id: str,
    full_rebuild: bool = False,
    bronze_manifest: BronzeBatchManifest | None = None,
    timings_ms: dict[str, int] | None = None,
) -> SilverBatchManifest:
    return SilverBuilder(spark, config, bronze_manifest, timings_ms).run(
        table_names,
        batch_id=batch_id,
        full_rebuild=full_rebuild,
    )
