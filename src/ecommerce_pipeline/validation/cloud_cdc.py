from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import read_delta
from ecommerce_pipeline.config.models import AppConfig, TableReference
from ecommerce_pipeline.contracts.cdc_tables import TYPED_CDC_TABLES
from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.control.gold_reconcile_queue import GOLD_RECONCILE_QUEUE_TABLE
from ecommerce_pipeline.control.gold_releases import GoldReleaseStore
from ecommerce_pipeline.ingestion.streaming.typed_bronze import QUARANTINE_TABLE_NAME


@dataclass(frozen=True)
class CloudCdcLakehouseReport:
    raw_rows: int
    raw_freshness_seconds: float | None
    typed_rows: int
    typed_freshness_seconds: float | None
    silver_rows: int
    silver_freshness_seconds: float | None
    gold_freshness_seconds: float
    quarantine_count: int
    pending_gold_count: int
    pending_gold_oldest_age_seconds: float | None
    gold_release_batch_id: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def validate_cloud_cdc_lakehouse(spark: SparkSession, config: AppConfig) -> CloudCdcLakehouseReport:
    """Validate one cloud canary without adding scans to every scheduled CDC cycle."""

    raw_reference = config.lakehouse.raw_cdc_bronze_reference(config.streaming.bronze_table)
    _require_table(spark, raw_reference, "Raw CDC Bronze")
    raw = read_delta(spark, raw_reference)
    _assert_unique(raw, ("_transport_event_id",), "raw CDC Bronze")
    raw_metrics = _rows_and_latest(raw, "ingested_at")

    typed_rows = 0
    typed_latest: datetime | None = None
    for table_name in TYPED_CDC_TABLES:
        reference = config.lakehouse.streaming_typed_bronze_reference(table_name)
        _require_table(spark, reference, f"typed Bronze {table_name}")
        typed = read_delta(spark, reference)
        _assert_unique(typed, ("_event_id",), f"typed Bronze {table_name}")
        count, latest = _rows_and_latest(typed, "_ingested_at")
        typed_rows += count
        typed_latest = _latest(typed_latest, latest)

    silver_rows = 0
    silver_latest: datetime | None = None
    for table_name, contract in SILVER_TABLES.items():
        reference = config.lakehouse.table_reference("silver", table_name)
        _require_table(spark, reference, f"Silver {table_name}")
        silver = read_delta(spark, reference)
        _assert_unique(silver, contract.primary_keys, f"Silver {table_name}")
        count, latest = _rows_and_latest(silver, "_silver_updated_at")
        silver_rows += count
        silver_latest = _latest(silver_latest, latest)

    quarantine_reference = config.lakehouse.streaming_typed_bronze_reference(QUARANTINE_TABLE_NAME)
    _require_table(spark, quarantine_reference, "CDC quarantine")
    quarantine_count = read_delta(spark, quarantine_reference).count()

    pending_count, pending_age = _pending_gold_metrics(spark, config)
    releases = GoldReleaseStore(spark, config)
    release = releases.latest()
    if release is None:
        raise RuntimeError("Gold has no published release after the CDC canary")
    if set(release.gold_versions) != set(GOLD_TABLES):
        raise RuntimeError("Gold release is missing contracted tables")
    if set(release.silver_versions) != set(SILVER_TABLES):
        raise RuntimeError("Gold release is missing contracted Silver progress")
    for table_name in GOLD_TABLES:
        _require_table(spark, config.lakehouse.table_reference("gold", table_name), f"Gold {table_name}")
    fact_reference = config.lakehouse.table_reference("gold", "fact_sales")
    history = _delta_table(spark, fact_reference).history(1).select("timestamp").first()
    if history is None or history["timestamp"] is None:
        raise RuntimeError("Gold fact_sales has no Delta history timestamp")

    return CloudCdcLakehouseReport(
        raw_rows=raw_metrics[0],
        raw_freshness_seconds=_age_seconds(raw_metrics[1]),
        typed_rows=typed_rows,
        typed_freshness_seconds=_age_seconds(typed_latest),
        silver_rows=silver_rows,
        silver_freshness_seconds=_age_seconds(silver_latest),
        gold_freshness_seconds=_age_seconds(history["timestamp"]) or 0.0,
        quarantine_count=quarantine_count,
        pending_gold_count=pending_count,
        pending_gold_oldest_age_seconds=pending_age,
        gold_release_batch_id=release.batch_id,
    )


def _pending_gold_metrics(spark: SparkSession, config: AppConfig) -> tuple[int, float | None]:
    reference = config.lakehouse.table_reference("silver", GOLD_RECONCILE_QUEUE_TABLE)
    if not _table_exists(spark, reference):
        return 0, None
    row = (
        read_delta(spark, reference)
        .where(F.col("status") == "pending")
        .agg(F.count(F.lit(1)).alias("count"), F.min("created_at").alias("oldest"))
        .first()
    )
    if row is None:
        return 0, None
    return int(row["count"]), _age_seconds(row["oldest"])


def _rows_and_latest(dataframe: DataFrame, timestamp_column: str) -> tuple[int, datetime | None]:
    row = dataframe.agg(
        F.count(F.lit(1)).alias("count"),
        F.max(timestamp_column).alias("latest"),
    ).first()
    if row is None:
        return 0, None
    return int(row["count"]), row["latest"]


def _assert_unique(dataframe: DataFrame, keys: tuple[str, ...], label: str) -> None:
    if dataframe.groupBy(*keys).count().where(F.col("count") > 1).take(1):
        raise RuntimeError(f"Duplicate key in {label}: {list(keys)}")


def _require_table(spark: SparkSession, reference: TableReference, label: str) -> None:
    if not _table_exists(spark, reference):
        raise RuntimeError(f"Missing {label}: {reference.value}")


def _table_exists(spark: SparkSession, reference: TableReference) -> bool:
    return (
        spark.catalog.tableExists(reference.value)
        if reference.is_catalog
        else DeltaTable.isDeltaTable(spark, reference.value)
    )


def _delta_table(spark: SparkSession, reference: TableReference) -> DeltaTable:
    return (
        DeltaTable.forName(spark, reference.value)
        if reference.is_catalog
        else DeltaTable.forPath(spark, reference.value)
    )


def _latest(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _age_seconds(timestamp: datetime | None) -> float | None:
    if timestamp is None:
        return None
    normalized = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)
    return round(max(0.0, (datetime.now(UTC) - normalized.astimezone(UTC)).total_seconds()), 3)


__all__ = ["CloudCdcLakehouseReport", "validate_cloud_cdc_lakehouse"]
