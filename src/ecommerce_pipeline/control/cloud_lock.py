from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from time import monotonic, sleep
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import _is_missing_delta_table, read_delta, write_delta
from ecommerce_pipeline.config.models import AppConfig, TableReference


class PipelineLockUnavailable(RuntimeError):
    """Raised when another cloud job owns the shared-layer writer lock."""


@contextmanager
def cloud_pipeline_lock(spark: SparkSession, config: AppConfig, owner: str) -> Iterator[None]:
    """Acquire one Delta-backed advisory lock using optimistic concurrency."""

    settings = config.coordination
    reference = config.lakehouse.coordination_reference()
    owner_id = f"{owner}-{uuid4().hex}"
    contender = spark.range(1).select(
        F.lit(settings.cloud_lock_name).alias("lock_name"),
        F.lit(owner_id).alias("owner_id"),
        F.current_timestamp().alias("acquired_at"),
        F.expr(f"current_timestamp() + INTERVAL {settings.cloud_lock_ttl_seconds} SECONDS").alias("expires_at"),
    )
    _ensure_lock_table(contender, reference)
    delta = _delta_table(spark, reference)
    deadline = monotonic() + settings.lock_wait_seconds
    current = None
    while True:
        try:
            (
                delta.alias("target")
                .merge(contender.alias("source"), "target.lock_name = source.lock_name")
                .whenMatchedUpdate(
                    condition="target.expires_at <= current_timestamp()",
                    set={
                        "owner_id": "source.owner_id",
                        "acquired_at": "source.acquired_at",
                        "expires_at": "source.expires_at",
                    },
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
            current = (
                read_delta(spark, reference)
                .where(F.col("lock_name") == settings.cloud_lock_name)
                .select("owner_id", "expires_at")
                .first()
            )
            if current is not None and current["owner_id"] == owner_id:
                break
        except Exception as exc:
            if not _is_concurrent_delta_error(exc):
                raise PipelineLockUnavailable(
                    f"Could not acquire cloud pipeline lock {settings.cloud_lock_name}"
                ) from exc
            current = None
        if monotonic() >= deadline:
            holder = "unknown" if current is None else current["owner_id"]
            raise PipelineLockUnavailable(
                f"Timed out waiting for cloud pipeline lock {settings.cloud_lock_name}; holder={holder}"
            )
        sleep(5)

    try:
        yield
    finally:
        delta.delete((F.col("lock_name") == settings.cloud_lock_name) & (F.col("owner_id") == owner_id))


def _ensure_lock_table(template: DataFrame, reference: TableReference) -> None:
    dataframe = template.limit(0)
    spark = dataframe.sparkSession
    if _lock_table_exists(spark, reference):
        return
    # A Unity Catalog external table can outlive its deleted ADLS directory.
    # Drop only this internal control-table registration before recreating it.
    if reference.is_catalog:
        spark.sql(f"DROP TABLE IF EXISTS {_catalog_identifier(reference.value)}")
    try:
        write_delta(dataframe.write.format("delta").mode("append"), reference)
    except Exception:
        # Another writer may have won the initial table-creation race.
        if not _lock_table_exists(spark, reference):
            raise


def _lock_table_exists(spark: SparkSession, reference: TableReference) -> bool:
    try:
        return (
            spark.catalog.tableExists(reference.value)
            if reference.is_catalog
            else DeltaTable.isDeltaTable(spark, reference.value)
        )
    except AnalysisException as exc:
        if _is_missing_delta_table(exc):
            return False
        raise


def _catalog_identifier(value: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split("."))


def _delta_table(spark: SparkSession, reference: TableReference) -> DeltaTable:
    return (
        DeltaTable.forName(spark, reference.value)
        if reference.is_catalog
        else DeltaTable.forPath(spark, reference.value)
    )


def _is_concurrent_delta_error(exc: Exception) -> bool:
    name = type(exc).__name__
    message = str(exc)
    return "Concurrent" in name or "DELTA_CONCURRENT" in message or "concurrent" in message.lower()


__all__ = ["PipelineLockUnavailable", "cloud_pipeline_lock"]
