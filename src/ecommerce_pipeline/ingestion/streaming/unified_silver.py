from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from threading import Lock
from time import perf_counter

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.storagelevel import StorageLevel

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_commit_metadata,
    delta_idempotent_transaction,
)
from ecommerce_pipeline.config.models import AppConfig, UnifiedSilverStreamingConfig
from ecommerce_pipeline.contracts.cdc_tables import TYPED_CDC_TABLES
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES, get_silver_contract
from ecommerce_pipeline.control.batch_runs import local_pipeline_lock
from ecommerce_pipeline.control.cloud_lock import cloud_pipeline_lock
from ecommerce_pipeline.control.gold_reconcile_queue import GoldReconcileQueue
from ecommerce_pipeline.ingestion.streaming.cdc_decoder import prepare_raw_cdc_events
from ecommerce_pipeline.ingestion.streaming.typed_bronze import TypedBronzeMaterializer
from ecommerce_pipeline.pipelines.build_gold import GoldBuilder
from ecommerce_pipeline.pipelines.build_silver import SILVER_CHANGE_HISTORY_PIPELINE_NAME, SilverBuilder
from ecommerce_pipeline.pipelines.quality import GoldQualityError
from ecommerce_pipeline.runtime.shutdown import unpersist_safely
from ecommerce_pipeline.transformations.silver.common import SILVER_SCHEMA_VERSION, SILVER_SEQUENCE_COLUMNS

CDC_SILVER_PIPELINE_NAME = "cdc_to_silver"
_REQUIRED_UNIFIED_METADATA = {
    "_is_deleted",
    "_event_occurred_at",
    "_ingestion_mode",
    "_ingestion_priority",
    "_source_event_sequence",
    "_source_event_subsequence",
}
_ORDER_SCOPED_GOLD_TABLES = frozenset(
    {
        "orders",
        "order_items",
        "order_vouchers",
        "payments",
        "shipments",
    }
)


class UnifiedSilverMaterializer:
    """Materialize typed CDC, then merge its physical rows into Unified Silver."""

    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        *,
        table_names: Sequence[str] | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.settings = config.streaming.silver
        self.table_names = tuple(TYPED_CDC_TABLES if table_names is None else table_names)
        unknown = sorted(set(self.table_names) - set(TYPED_CDC_TABLES))
        if unknown:
            raise ValueError(f"Unknown CDC Silver tables: {unknown}")
        self.lakehouse = LakehouseAdapter(spark, config)
        self.silver = SilverBuilder(spark, config)
        self.typed_bronze = TypedBronzeMaterializer(spark, config)
        self.gold_queue = GoldReconcileQueue(spark, config)
        # foreachBatch runs on Spark's streaming callback thread while Gold is
        # driven by the control thread. Do not let two memory-heavy Spark jobs
        # from the same query overlap before either reaches the writer lock.
        self._cycle_lock = Lock()
        self._targets_validated = False

    def process_batch(self, raw_batch: DataFrame, batch_id: int) -> None:
        with self._cycle_lock:
            self._process_batch(raw_batch, batch_id)

    def _process_batch(self, raw_batch: DataFrame, batch_id: int) -> None:
        batch_started = perf_counter()
        prepared = prepare_raw_cdc_events(raw_batch).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            bronze_started = perf_counter()
            materialized = self.typed_bronze.materialize(prepared, batch_id)
            bronze_seconds = perf_counter() - bronze_started
        finally:
            unpersist_safely(prepared, label="unified-silver")
        statistics = materialized.statistics
        changed_tables = [name for name in self.table_names if name in statistics]
        if not changed_tables:
            print(
                f"[unified-silver-batch] batch={batch_id} records=0 tables=0 "
                f"quarantined={materialized.quarantined_count} typed_bronze={bronze_seconds:.2f}s",
                flush=True,
            )
            return

        transformed = {
            table_name: self._transform_table(materialized.tables[table_name], table_name)
            for table_name in changed_tables
        }
        histories = {
            table_name: self._transform_history_table(materialized.tables[table_name], table_name)
            for table_name in changed_tables
            if get_silver_contract(table_name).materialize_change_history
        }
        with self._writer_lock(f"stream-batch-{batch_id}"):
            target_exists = self._preflight_targets(transformed)
            silver_started = perf_counter()

            def merge_table(table_name: str) -> tuple[str, float]:
                table_started = perf_counter()
                if table_name in histories:
                    contract = get_silver_contract(table_name)
                    history_app_id = (
                        f"{SILVER_CHANGE_HISTORY_PIPELINE_NAME}:"
                        f"{self.settings.query_name}:{self.settings.checkpoint_version}:{table_name}"
                    )
                    self.silver.append_change_history(
                        contract,
                        histories[table_name],
                        batch_id=f"cdc-stream-{batch_id}",
                        bronze_version=batch_id,
                        bronze_starting_version=None,
                        transaction_version=batch_id,
                        pipeline_name=SILVER_CHANGE_HISTORY_PIPELINE_NAME,
                        transaction_app_id=history_app_id,
                    )
                self._merge_table(
                    transformed[table_name],
                    table_name,
                    batch_id,
                    statistics[table_name],
                    target_exists=target_exists[table_name],
                )
                return table_name, perf_counter() - table_started

            # A new table is written through ``dataframe.write`` and therefore
            # uses the DataFrame's owning SparkSession. Bootstrap sequentially
            # so transaction/user metadata cannot race on that shared session.
            # Existing targets use independent sessions and remain parallel.
            bootstrap_required = any(not target_exists[name] for name in changed_tables)
            workers = (
                1 if bootstrap_required else max(1, min(len(changed_tables), self.config.spark.max_parallel_tables))
            )
            with ThreadPoolExecutor(max_workers=workers) as executor:
                table_seconds = dict(executor.map(merge_table, changed_tables))
            silver_seconds = perf_counter() - silver_started
            affected_order_ids, requires_fact_readiness = self._gold_scope(transformed)
            queue_started = perf_counter()
            self.gold_queue.enqueue(
                request_id=f"{self.settings.query_name}:{self.settings.checkpoint_version}:{batch_id}",
                affected_order_ids=affected_order_ids,
                requires_fact_readiness=requires_fact_readiness,
            )
            queue_seconds = perf_counter() - queue_started

            self._targets_validated = True
        slowest_table = max(table_seconds, key=table_seconds.__getitem__)
        records = sum(int(statistics[name]["record_count"]) for name in changed_tables)
        print(
            f"[unified-silver-batch] batch={batch_id} records={records} tables={len(changed_tables)} "
            f"quarantined={materialized.quarantined_count} typed_bronze={bronze_seconds:.2f}s "
            f"silver={silver_seconds:.2f}s gold_queue={queue_seconds:.2f}s "
            f"slowest={slowest_table}:{table_seconds[slowest_table]:.2f}s "
            "gold_status=queued "
            f"total={perf_counter() - batch_started:.2f}s",
            flush=True,
        )

    def reconcile_pending_gold_if_ready(self, *, batch_id: str) -> str:
        """Publish one durable pending Silver scope without blocking Silver micro-batches."""

        with self._cycle_lock:
            return self._reconcile_pending_gold_if_ready(batch_id=batch_id)

    def _reconcile_pending_gold_if_ready(self, *, batch_id: str) -> str:
        # Avoid acquiring the shared writer lock on every idle poll. Re-read after
        # acquiring it so another writer cannot invalidate the selected scope.
        if self.gold_queue.pending(max_order_ids=self.settings.max_gold_readiness_order_ids) is None:
            return "not_pending"
        started = perf_counter()
        with self._writer_lock(f"gold-{batch_id}"):
            pending = self.gold_queue.pending(max_order_ids=self.settings.max_gold_readiness_order_ids)
            if pending is None:
                return "not_pending"
            if pending.requires_fact_readiness and not self._source_fact_ready_for_gold(pending.affected_order_ids):
                return "deferred_source_incomplete"
            status = self._run_gold_locked(
                batch_id=batch_id,
                raise_on_quality_error=False,
                rebuild_on_quality_error=False,
            )
            if status == "published":
                self.gold_queue.complete(pending.keys)
            print(
                f"[gold-reconcile-cycle] status={status} queue_rows={len(pending.keys)} "
                f"elapsed={perf_counter() - started:.2f}s",
                flush=True,
            )
            return status

    def reconcile_gold(
        self,
        *,
        batch_id: str = "cdc-final-reconcile",
        raise_on_quality_error: bool = True,
        rebuild_on_quality_error: bool = True,
        defer_if_source_incomplete: bool = False,
        affected_order_ids: set[int] | None = None,
    ) -> str:
        """Build Gold from Unified Silver after CDC has committed Silver changes."""

        if defer_if_source_incomplete and not self._source_fact_ready_for_gold(affected_order_ids):
            return "deferred_source_incomplete"
        with self._writer_lock(f"gold-{batch_id}"):
            return self._run_gold_locked(
                batch_id=batch_id,
                raise_on_quality_error=raise_on_quality_error,
                rebuild_on_quality_error=rebuild_on_quality_error,
            )

    def _run_gold_locked(
        self,
        *,
        batch_id: str,
        raise_on_quality_error: bool,
        rebuild_on_quality_error: bool,
    ) -> str:
        try:
            GoldBuilder(self.spark, self.config).run(batch_id=batch_id)
        except GoldQualityError as exc:
            if not rebuild_on_quality_error:
                print(
                    f"[gold-quality-alert] action=skip_publish_keep_streaming error={exc}",
                    flush=True,
                )
                if raise_on_quality_error:
                    raise
                return "quality_skipped"
            print(
                f"[gold-quality-alert] action=full_rebuild_after_failed_candidate error={exc}",
                flush=True,
            )
            try:
                GoldBuilder(self.spark, self.config).run(
                    batch_id=f"{batch_id}-rebuild",
                    full_rebuild=True,
                )
            except GoldQualityError as rebuild_exc:
                print(
                    f"[gold-quality-alert] action=skip_publish_keep_streaming error={rebuild_exc}",
                    flush=True,
                )
                if raise_on_quality_error:
                    raise
                return "quality_skipped"
            except Exception as rebuild_error:
                print(
                    f"[cdc-alert] metric=gold_publish_failure value=1 "
                    f"error_type={type(rebuild_error).__name__} error={rebuild_error}",
                    flush=True,
                )
                raise
        except Exception as publish_error:
            print(
                f"[cdc-alert] metric=gold_publish_failure value=1 "
                f"error_type={type(publish_error).__name__} error={publish_error}",
                flush=True,
            )
            raise
        print("[gold-reconcile] status=published source=unified_silver", flush=True)
        return "published"

    def _gold_scope(self, transformed: Mapping[str, DataFrame]) -> tuple[set[int] | None, bool]:
        order_scoped = {
            table_name: dataframe
            for table_name, dataframe in transformed.items()
            if table_name in _ORDER_SCOPED_GOLD_TABLES
        }
        if not order_scoped:
            return set(), False
        affected = self._collect_affected_order_ids(order_scoped)
        return affected, True

    def _collect_affected_order_ids(self, transformed: Mapping[str, DataFrame]) -> set[int] | None:
        max_order_ids = self.settings.max_gold_readiness_order_ids
        affected: set[int] = set()
        for dataframe in transformed.values():
            if "order_id" not in dataframe.columns:
                continue
            remaining = max_order_ids + 1 - len(affected)
            rows = (
                dataframe.select("order_id").where(F.col("order_id").isNotNull()).distinct().limit(remaining).collect()
            )
            affected.update(int(row["order_id"]) for row in rows)
            if len(affected) > max_order_ids:
                return None
        return affected

    def _source_fact_ready_for_gold(self, affected_order_ids: set[int] | None = None) -> bool:
        if affected_order_ids is not None and not affected_order_ids:
            return True
        # Read physical Silver state here: tombstones distinguish a completed DELETE
        # from a source row that has not arrived yet. Business readers hide them.
        orders = self.lakehouse.read_table("silver", "orders", include_deleted=True).select(
            "order_id",
            "_is_deleted",
            "subtotal_amount",
            "tax_amount",
        )
        if affected_order_ids is not None:
            scoped_orders = self.spark.createDataFrame(
                [(order_id,) for order_id in sorted(affected_order_ids)],
                "order_id long",
            )
            orders = scoped_orders.join(orders, "order_id", "left")
        else:
            orders = orders.filter(~F.col("_is_deleted"))

        item_totals = (
            self.lakehouse.read_table("silver", "order_items", include_deleted=True)
            .filter(~F.col("_is_deleted"))
            .groupBy("order_id")
            .agg(
                F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)").alias("actual_gross"),
                F.sum("tax_amount").cast("decimal(18,2)").alias("actual_tax"),
            )
        )
        if affected_order_ids is not None:
            item_totals = scoped_orders.join(item_totals, "order_id", "left")
        tolerance = F.lit("0.01").cast("decimal(18,2)")
        active_order_incomplete = F.col("_is_deleted").isNull() | (
            ~F.col("_is_deleted")
            & (
                F.col("actual_gross").isNull()
                | (F.abs(F.col("subtotal_amount") - F.col("actual_gross")) > tolerance)
                | (F.abs(F.col("tax_amount") - F.col("actual_tax")) > tolerance)
            )
        )
        deleted_order_incomplete = F.col("_is_deleted") & F.col("actual_gross").isNotNull()
        incomplete = (
            orders.join(item_totals, "order_id", "left")
            .filter(active_order_incomplete | deleted_order_incomplete)
            .select(
                F.to_json(
                    F.struct(
                        F.col("order_id").alias("source_order_id"),
                        "_is_deleted",
                        "subtotal_amount",
                        "actual_gross",
                        "tax_amount",
                        "actual_tax",
                    )
                ).alias("details")
            )
            .limit(3)
            .collect()
        )
        if not incomplete:
            return True
        examples = [row["details"] for row in incomplete]
        print(
            f"[gold-reconcile] status=deferred reason=source_fact_incomplete examples={examples}",
            flush=True,
        )
        return False

    def _writer_lock(self, owner: str) -> AbstractContextManager[None]:
        if self.config.spark.master:
            return local_pipeline_lock(
                self.config.coordination.local_lock_path,
                owner,
                wait_timeout_seconds=self.config.coordination.lock_wait_seconds,
            )
        return cloud_pipeline_lock(self.spark, self.config, owner)

    def _transform_table(self, typed_bronze: DataFrame, table_name: str) -> DataFrame:
        return self.silver.transform(get_silver_contract(table_name), typed_bronze)

    def _transform_history_table(self, typed_bronze: DataFrame, table_name: str) -> DataFrame:
        return self.silver.transform_history(get_silver_contract(table_name), typed_bronze)

    def _preflight_targets(self, transformed: Mapping[str, DataFrame]) -> dict[str, bool]:
        if self._targets_validated:
            return dict.fromkeys(SILVER_TABLES, True)

        def inspect_target(table_name: str) -> tuple[str, tuple[bool, set[str]]]:
            worker = LakehouseAdapter(self.spark.newSession(), self.config)
            exists = worker.table_exists("silver", table_name)
            columns = set(worker.read_table("silver", table_name).columns) if exists else set()
            return table_name, (exists, columns)

        workers = max(1, min(len(SILVER_TABLES), self.config.spark.max_parallel_tables))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            targets = dict(executor.map(inspect_target, SILVER_TABLES))
        target_exists = {table_name: state[0] for table_name, state in targets.items()}
        missing_without_source = sorted(
            table_name for table_name, exists in target_exists.items() if not exists and table_name not in transformed
        )
        if missing_without_source:
            raise RuntimeError(
                "Unified Silver bootstrap is incomplete; run the batch bootstrap or replay a complete raw CDC "
                f"snapshot. Missing tables: {missing_without_source}"
            )
        for table_name, exists in target_exists.items():
            if not exists:
                continue
            target_columns = targets[table_name][1]
            missing_metadata = sorted(_REQUIRED_UNIFIED_METADATA - target_columns)
            if missing_metadata:
                raise RuntimeError(
                    f"Silver schema is outdated for {table_name}; missing={missing_metadata}. "
                    "Rebuild Unified Silver before starting CDC."
                )
            if table_name in transformed:
                source_columns = set(transformed[table_name].columns)
                if source_columns != target_columns:
                    raise RuntimeError(
                        f"Unified Silver schema mismatch for {table_name}; "
                        f"missing_from_source={sorted(target_columns - source_columns)}, "
                        f"missing_from_target={sorted(source_columns - target_columns)}"
                    )
        return target_exists

    def _merge_table(
        self,
        dataframe: DataFrame,
        table_name: str,
        batch_id: int,
        statistics: Row,
        *,
        target_exists: bool,
    ) -> None:
        # ``DataFrame.write`` uses dataframe.sparkSession. For the initial
        # table creation the commit metadata and idempotent transaction must be
        # configured on that same session. MERGE targets can safely use an
        # isolated session and retain table-level parallelism.
        worker_spark = self.spark if not target_exists else self.spark.newSession()
        lakehouse = LakehouseAdapter(worker_spark, self.config)
        contract = get_silver_contract(table_name)
        metadata: dict[str, object] = {
            "pipeline": CDC_SILVER_PIPELINE_NAME,
            "source_table": table_name,
            "stream_batch_id": batch_id,
            "record_count": int(statistics["record_count"]),
            "min_source_lsn": statistics["min_source_lsn"],
            "max_source_lsn": statistics["max_source_lsn"],
            "silver_schema_version": SILVER_SCHEMA_VERSION,
        }
        transaction_app_id = f"{self.settings.query_name}-{self.settings.checkpoint_version}-{table_name}"
        with (
            delta_idempotent_transaction(
                worker_spark,
                application_id=transaction_app_id,
                transaction_version=batch_id,
            ),
            delta_commit_metadata(worker_spark, metadata),
        ):
            if not target_exists:
                lakehouse.write_table(
                    dataframe,
                    "silver",
                    table_name,
                    enable_change_data_feed=True,
                )
                return
            lakehouse.upsert_table(
                dataframe,
                "silver",
                table_name,
                contract.primary_keys,
                delete_mode="soft",
                sequence_columns=SILVER_SEQUENCE_COLUMNS,
                target_exists=True,
                source_is_nonempty=True,
                preserve_target_on_soft_delete=True,
            )


def start_unified_silver_stream(
    raw_stream: DataFrame,
    materializer: UnifiedSilverMaterializer,
    settings: UnifiedSilverStreamingConfig,
    checkpoint_location: str,
    *,
    available_now: bool = False,
) -> StreamingQuery:
    writer = (
        raw_stream.writeStream.queryName(settings.query_name)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(materializer.process_batch)
    )
    writer = (
        writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=settings.trigger_interval)
    )
    return writer.start()


__all__ = [
    "CDC_SILVER_PIPELINE_NAME",
    "UnifiedSilverMaterializer",
    "start_unified_silver_stream",
]
