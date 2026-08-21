from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.config.models import AppConfig

GOLD_RECONCILE_QUEUE_TABLE = "gold_reconcile_queue"


@dataclass(frozen=True)
class PendingGoldScope:
    keys: tuple[tuple[str, str], ...]
    affected_order_ids: set[int] | None
    requires_fact_readiness: bool


class GoldReconcileQueue:
    """Durable, retry-safe hand-off between Silver commits and Gold publication."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.lakehouse = LakehouseAdapter(spark, config)

    def enqueue(
        self,
        *,
        request_id: str,
        affected_order_ids: set[int] | None,
        requires_fact_readiness: bool,
    ) -> None:
        now = datetime.now(UTC).replace(tzinfo=None)
        if affected_order_ids is None:
            scopes: list[tuple[str, int | None]] = [("all-orders", None)]
        elif affected_order_ids:
            valid_ids = sorted(order_id for order_id in affected_order_ids if order_id > 0)
            invalid_ids = sorted(affected_order_ids - set(valid_ids))
            if invalid_ids:
                print(
                    f"[gold-queue-quality-alert] action=full_scope invalid_order_ids={invalid_ids[:10]}",
                    flush=True,
                )
                scopes = [("all-orders", None)]
            else:
                scopes = [(f"order:{order_id}", order_id) for order_id in valid_ids]
        else:
            scopes = [("metadata-only", None)]
        rows = [
            (request_id, scope_key, order_id, requires_fact_readiness, "pending", now, now)
            for scope_key, order_id in scopes
        ]
        dataframe = self.spark.createDataFrame(
            rows,
            "request_id string, scope_key string, order_id long, requires_fact_readiness boolean, "
            "status string, created_at timestamp, updated_at timestamp",
        )
        exists = self.lakehouse.table_exists("silver", GOLD_RECONCILE_QUEUE_TABLE)
        if not exists:
            self.lakehouse.write_table(dataframe, "silver", GOLD_RECONCILE_QUEUE_TABLE)
            return
        self.lakehouse.upsert_table(
            dataframe,
            "silver",
            GOLD_RECONCILE_QUEUE_TABLE,
            ("request_id", "scope_key"),
            target_exists=True,
            source_is_nonempty=True,
        )

    def pending(self, *, max_order_ids: int) -> PendingGoldScope | None:
        if not self.lakehouse.table_exists("silver", GOLD_RECONCILE_QUEUE_TABLE):
            return None
        rows = (
            self.lakehouse.read_table("silver", GOLD_RECONCILE_QUEUE_TABLE)
            .where(F.col("status") == "pending")
            .select("request_id", "scope_key", "order_id", "requires_fact_readiness")
            .limit(max_order_ids + 1)
            .collect()
        )
        if not rows:
            return None
        keys = tuple((str(row["request_id"]), str(row["scope_key"])) for row in rows)
        requires_readiness = any(bool(row["requires_fact_readiness"]) for row in rows)
        full_scope = any(row["scope_key"] == "all-orders" for row in rows) or len(rows) > max_order_ids
        order_ids = {int(row["order_id"]) for row in rows if row["order_id"] is not None and int(row["order_id"]) > 0}
        return PendingGoldScope(
            keys=keys,
            affected_order_ids=None if full_scope else order_ids,
            requires_fact_readiness=requires_readiness,
        )

    def complete(self, keys: tuple[tuple[str, str], ...]) -> None:
        if not keys:
            return
        now = datetime.now(UTC).replace(tzinfo=None)
        key_rows = self.spark.createDataFrame(keys, "request_id string, scope_key string")
        completed = (
            key_rows.join(
                self.lakehouse.read_table("silver", GOLD_RECONCILE_QUEUE_TABLE),
                ["request_id", "scope_key"],
            )
            .withColumn("status", F.lit("published"))
            .withColumn("updated_at", F.lit(now))
        )
        self.lakehouse.upsert_table(
            completed,
            "silver",
            GOLD_RECONCILE_QUEUE_TABLE,
            ("request_id", "scope_key"),
            target_exists=True,
            source_is_nonempty=True,
        )


__all__ = ["GOLD_RECONCILE_QUEUE_TABLE", "GoldReconcileQueue", "PendingGoldScope"]
