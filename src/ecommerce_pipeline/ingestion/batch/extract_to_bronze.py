from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import (
    DeltaTableState,
    delta_commit_metadata,
    read_delta,
    set_delta_table_property,
    try_delta_table_state,
)
from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import AppConfig, TableReference
from ecommerce_pipeline.contracts.bronze_tables import (
    BRONZE_SCHEMA_VERSION,
    BRONZE_TABLES,
    BronzeTableContract,
    get_bronze_contract,
)
from ecommerce_pipeline.contracts.change_events import ChangeOperation, EventCursor
from ecommerce_pipeline.control.manifests import BronzeBatchManifest, BronzeTableResult
from ecommerce_pipeline.ingestion.batch.bronze_operations import (
    ChangeEventStats,
    add_bronze_metadata,
    change_event_query,
    collect_change_event_stats,
    jdbc_partition_count,
    read_batch_upper_bounds,
    write_append_only,
)


@dataclass(frozen=True)
class _BronzeState:
    table_name: str
    reference: TableReference
    delta_state: DeltaTableState | None
    cursor: EventCursor | None
    processed_version: int


class BronzeExtractor:
    def __init__(self, spark: SparkSession, config: AppConfig, batch_id: str) -> None:
        self.spark = spark
        self.config = config
        self.batch_id = batch_id
        self.reader = PostgresReader(spark, config)

    def run(self, table_names: list[str]) -> BronzeBatchManifest:
        states = [self._table_state(table_name) for table_name in table_names]
        upper_bounds = read_batch_upper_bounds(
            self.reader,
            self.config,
            {state.table_name: state.cursor for state in states},
        )
        results = [self._run_table(state, upper_bounds[state.table_name]) for state in states]
        return BronzeBatchManifest.from_results(self.batch_id, results)

    def _table_state(self, table_name: str) -> _BronzeState:
        reference = self.config.lakehouse.table_reference("bronze", table_name)
        state = try_delta_table_state(self.spark, reference, pipeline="postgres_to_bronze")
        cursor, processed_version = self._read_cursor(table_name, reference, state)
        return _BronzeState(table_name, reference, state, cursor, processed_version)

    def _run_table(
        self,
        state: _BronzeState,
        upper_bound: int | None,
    ) -> BronzeTableResult:
        table_name = state.table_name
        table_exists = state.delta_state is not None
        previous = state.cursor
        contract = get_bronze_contract(table_name)
        if table_exists and upper_bound is None:
            return self._result(
                state,
                previous,
                None,
                0,
                {operation.value: 0 for operation in ChangeOperation},
            )

        source_df, stats = self._materialize_events(contract, previous, upper_bound)
        try:
            current = (
                EventCursor(stats.last_event_id) if stats.last_event_id is not None else previous or EventCursor(0)
            )
            if stats.record_count > 0 or not table_exists:
                write_append_only(
                    self.spark,
                    source_df,
                    self.config,
                    state.reference,
                    table_exists,
                    table_name=table_name,
                    batch_id=self.batch_id,
                    cursor=current,
                )
        finally:
            source_df.unpersist()

        return self._result(
            state,
            current,
            upper_bound,
            stats.record_count,
            stats.operation_counts,
        )

    def _result(
        self,
        state: _BronzeState,
        current: EventCursor | None,
        upper_bound: int | None,
        record_count: int,
        operation_counts: dict[str, int],
    ) -> BronzeTableResult:
        table_exists = state.delta_state is not None
        wrote_commit = record_count > 0 or not table_exists
        physical_version = -1 if state.delta_state is None else max(state.delta_state.version, state.processed_version)
        delta_version = physical_version + 1 if wrote_commit else state.processed_version
        return BronzeTableResult(
            batch_id=self.batch_id,
            table_name=state.table_name,
            output_path=state.reference.value,
            record_count=record_count,
            ingestion_type="incremental" if table_exists else "initial",
            delta_version=delta_version,
            previous_delta_version=physical_version,
            operation_counts=operation_counts,
            schema_version=BRONZE_SCHEMA_VERSION,
            batch_upper_bound_event_id=upper_bound,
            previous_event_id=state.cursor.last_event_id if state.cursor else None,
            current_event_id=current.last_event_id if current else None,
        )

    def _read_events(
        self,
        contract: BronzeTableContract,
        cursor: EventCursor | None,
        upper_bound: int | None,
    ) -> DataFrame:
        query = change_event_query(self.config, contract, cursor, upper_bound)
        if upper_bound is None:
            raw_df = self.reader.read_query(query)
        else:
            lower_bound = cursor.last_event_id if cursor else 0
            raw_df = self.reader.read_partitioned_query(
                query,
                partition_column="_event_id",
                lower_bound=lower_bound + 1,
                upper_bound=upper_bound + 1,
                num_partitions=jdbc_partition_count(self.config, cursor, upper_bound),
            )
        if contract.excluded_columns:
            raw_df = raw_df.drop(*contract.excluded_columns)
        return add_bronze_metadata(raw_df, self.config, contract.table_name, self.batch_id)

    def _materialize_events(
        self,
        contract: BronzeTableContract,
        cursor: EventCursor | None,
        upper_bound: int | None,
    ) -> tuple[DataFrame, ChangeEventStats]:
        def materialize() -> tuple[DataFrame, ChangeEventStats]:
            source_df = self._read_events(contract, cursor, upper_bound).cache()
            try:
                stats = collect_change_event_stats(source_df, contract)
                return source_df, stats
            except Exception:
                source_df.unpersist()
                raise

        return self.reader.with_retry(materialize, operation=f"JDBC extraction for {contract.table_name}")

    def _read_cursor(
        self,
        table_name: str,
        reference: TableReference,
        state: DeltaTableState | None,
    ) -> tuple[EventCursor | None, int]:
        if state is None:
            return None, -1
        metadata = state.progress
        if metadata is None:
            return self._migrate_legacy_cursor(table_name, reference), state.version + 1
        last_event_id = metadata.get("last_event_id")
        if not isinstance(last_event_id, int) or isinstance(last_event_id, bool) or last_event_id < 0:
            raise ValueError(f"Invalid Bronze Delta progress metadata for table: {table_name}")
        if state.progress_version is None:
            raise RuntimeError(f"Bronze progress version is missing for table: {table_name}")
        return EventCursor(last_event_id), state.progress_version

    def _migrate_legacy_cursor(self, table_name: str, reference: TableReference) -> EventCursor:
        bronze = read_delta(self.spark, reference)
        if "_event_id" not in bronze.columns:
            raise RuntimeError(
                f"Bronze schema is outdated for {table_name}; rebuild Bronze before change-event ingestion"
            )
        row = bronze.agg(F.max("_event_id").alias("last_event_id")).first()
        cursor = EventCursor(0 if row is None or row["last_event_id"] is None else int(row["last_event_id"]))
        metadata: dict[str, object] = {
            "pipeline": "postgres_to_bronze",
            "source_table": table_name,
            "batch_id": self.batch_id,
            "last_event_id": cursor.last_event_id,
            "bronze_schema_version": BRONZE_SCHEMA_VERSION,
            "migration": "legacy_bronze_cursor",
        }
        with delta_commit_metadata(self.spark, metadata):
            set_delta_table_property(
                self.spark,
                reference,
                "pipeline.bronzeProgressMigration",
                self.batch_id,
            )
        return cursor


def extract_all_to_bronze(
    spark: SparkSession,
    config: AppConfig,
    batch_id: str,
    table_names: list[str] | None = None,
) -> BronzeBatchManifest:
    tables = list(BRONZE_TABLES) if table_names is None else table_names
    unknown = sorted(set(tables) - set(BRONZE_TABLES))
    if unknown:
        raise ValueError(f"Unknown source tables: {unknown}")
    extractor = BronzeExtractor(spark, config, batch_id)
    return extractor.run(tables)
