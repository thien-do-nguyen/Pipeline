from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter


@dataclass(frozen=True)
class GoldQualityReport:
    source_order_rows: int
    source_item_rows: int
    fact_rows: int
    gross_sales_total: str
    discount_total: str
    tax_total: str
    shipping_total: str
    net_sales_total: str


class GoldQualityChecker:
    DIMENSION_KEYS = {
        "dim_date": ("date_key", "order_date_key"),
        "dim_time": ("time_key", "order_time_key"),
        "dim_customer": ("customer_key", "customer_key"),
        "dim_product": ("product_key", "product_key"),
        "dim_shop": ("shop_key", "shop_key"),
        "dim_category": ("category_key", "category_key"),
        "dim_promotion": ("promotion_key", "promotion_key"),
        "dim_payment": ("payment_key", "payment_key"),
        "dim_shipping": ("shipping_key", "shipping_key"),
    }
    REQUIRED_NON_UNKNOWN_KEYS = (
        "order_date_key",
        "order_time_key",
        "customer_key",
        "product_key",
        "shop_key",
        "category_key",
        "ship_to_location_key",
        "bill_to_location_key",
    )

    def __init__(self, lakehouse: LakehouseAdapter) -> None:
        self.lakehouse = lakehouse

    def run(self, sources: Mapping[str, DataFrame] | None = None) -> GoldQualityReport:
        fact = self.lakehouse.read_table("gold", "fact_sales")
        self._assert_unique(fact, ["source_order_id", "source_order_item_id"], "fact_sales")
        self._assert_unique(fact, ["sales_key"], "fact_sales")
        self._assert_valid_amounts(fact)
        self._assert_required_keys(fact)
        for dimension, (dimension_key, fact_key) in self.DIMENSION_KEYS.items():
            self._assert_unique(self.lakehouse.read_table("gold", dimension), [dimension_key], dimension)
            self._assert_reference(fact, dimension, dimension_key, fact_key)
        self._assert_unique(self.lakehouse.read_table("gold", "dim_location"), ["location_key"], "dim_location")
        self._assert_location_reference(fact, "ship_to_location_key")
        self._assert_location_reference(fact, "bill_to_location_key")
        self._assert_customer_scd2()

        if sources is None:
            sources = {
                "orders": self.lakehouse.read_table("silver", "orders"),
                "order_items": self.lakehouse.read_table("silver", "order_items"),
            }
        return self._assert_reconciliation(fact, sources["orders"], sources["order_items"])

    def _assert_unique(self, df: DataFrame, keys: list[str], table_name: str) -> None:
        duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1)
        if self._has_rows(duplicates):
            raise ValueError(f"Duplicate key in gold.{table_name}: {keys}")

    def _assert_valid_amounts(self, fact: DataFrame) -> None:
        amounts = [
            "unit_price_amount",
            "gross_sales_amount",
            "line_discount_amount",
            "order_discount_amount_allocated",
            "total_discount_amount",
            "tax_amount",
            "shipping_amount_allocated",
            "net_sales_amount",
        ]
        invalid_condition = F.col("quantity").isNull() | (F.col("quantity") <= 0)
        for name in amounts:
            invalid_condition = invalid_condition | F.col(name).isNull() | (F.col(name) < 0)
        if self._has_rows(fact.filter(invalid_condition)):
            raise ValueError("Gold fact contains null/invalid quantity or monetary amounts")

    def _assert_required_keys(self, fact: DataFrame) -> None:
        condition = F.lit(False)
        for key in self.REQUIRED_NON_UNKNOWN_KEYS:
            condition = condition | F.col(key).isNull() | (F.col(key) == 0)
        if self._has_rows(fact.filter(condition)):
            raise ValueError("Gold fact contains unresolved required dimension keys")

    def _assert_reference(self, fact: DataFrame, dimension: str, dimension_key: str, fact_key: str) -> None:
        dimension_keys = (
            self.lakehouse.read_table("gold", dimension).select(F.col(dimension_key).alias(fact_key)).distinct()
        )
        missing = fact.select(fact_key).distinct().join(dimension_keys, fact_key, "left_anti")
        if self._has_rows(missing):
            raise ValueError(f"Missing gold.{dimension} reference for {fact_key}")

    def _assert_location_reference(self, fact: DataFrame, fact_key: str) -> None:
        locations = self.lakehouse.read_table("gold", "dim_location").select(F.col("location_key").alias(fact_key))
        missing = fact.select(fact_key).distinct().join(locations, fact_key, "left_anti")
        if self._has_rows(missing):
            raise ValueError(f"Missing gold.dim_location reference for {fact_key}")

    def _assert_customer_scd2(self) -> None:
        customers = self.lakehouse.read_table("gold", "dim_customer").filter(F.col("source_customer_id").isNotNull())
        all_members = customers.select("source_customer_id").distinct()
        current_counts = customers.filter("is_current").groupBy("source_customer_id").count()
        invalid_current = all_members.join(current_counts, "source_customer_id", "left").filter(
            F.coalesce(F.col("count"), F.lit(0)) != 1
        )
        invalid_range = customers.filter(F.col("effective_from") >= F.col("effective_to"))
        history = Window.partitionBy("source_customer_id").orderBy("effective_from", "customer_key")
        overlap = customers.withColumn("_previous_to", F.lag("effective_to").over(history)).filter(
            F.col("_previous_to").isNotNull() & (F.col("effective_from") < F.col("_previous_to"))
        )
        if self._has_rows(invalid_current) or self._has_rows(invalid_range) or self._has_rows(overlap):
            raise ValueError("Invalid SCD2 history in gold.dim_customer")

    def _assert_reconciliation(self, fact: DataFrame, orders: DataFrame, items: DataFrame) -> GoldQualityReport:
        source_order_rows = orders.count()
        source_item_rows = items.count()
        fact_rows = fact.count()
        if source_item_rows != fact_rows:
            raise ValueError(f"Fact row count mismatch: source_items={source_item_rows}, fact_rows={fact_rows}")

        source_keys = items.select(
            F.col("order_id").alias("source_order_id"), F.col("order_item_id").alias("source_order_item_id")
        )
        fact_keys = fact.select("source_order_id", "source_order_item_id")
        if self._has_rows(source_keys.join(fact_keys, ["source_order_id", "source_order_item_id"], "left_anti")):
            raise ValueError("Gold fact is missing one or more source order-item keys")

        actual = fact.groupBy("source_order_id").agg(
            F.sum("gross_sales_amount").alias("actual_gross"),
            F.sum("total_discount_amount").alias("actual_discount"),
            F.sum("tax_amount").alias("actual_tax"),
            F.sum("shipping_amount_allocated").alias("actual_shipping"),
            F.sum("net_sales_amount").alias("actual_net"),
        )
        expected = orders.select(
            F.col("order_id").alias("source_order_id"),
            F.col("subtotal_amount").alias("expected_gross"),
            F.col("discount_amount").alias("expected_discount"),
            F.col("tax_amount").alias("expected_tax"),
            F.col("shipping_amount").alias("expected_shipping"),
            F.col("total_amount").alias("expected_net"),
        )
        reconciled = expected.join(actual, "source_order_id", "left")
        tolerance = F.lit("0.01").cast("decimal(18,2)")
        invalid = reconciled.filter(
            F.col("actual_net").isNull()
            | (F.abs(F.col("expected_gross") - F.col("actual_gross")) > tolerance)
            | (F.abs(F.col("expected_discount") - F.col("actual_discount")) > tolerance)
            | (F.abs(F.col("expected_tax") - F.col("actual_tax")) > tolerance)
            | (F.abs(F.col("expected_shipping") - F.col("actual_shipping")) > tolerance)
            | (F.abs(F.col("expected_net") - F.col("actual_net")) > tolerance)
        )
        if self._has_rows(invalid):
            examples = [row.asDict() for row in invalid.limit(3).collect()]
            raise ValueError(f"Source-to-Gold monetary reconciliation failed: {examples}")

        totals = fact.agg(
            F.sum("gross_sales_amount").alias("gross"),
            F.sum("total_discount_amount").alias("discount"),
            F.sum("tax_amount").alias("tax"),
            F.sum("shipping_amount_allocated").alias("shipping"),
            F.sum("net_sales_amount").alias("net"),
        ).first()
        if totals is None:
            raise ValueError("Gold fact totals are unavailable")
        return GoldQualityReport(
            source_order_rows=source_order_rows,
            source_item_rows=source_item_rows,
            fact_rows=fact_rows,
            gross_sales_total=str(totals["gross"]),
            discount_total=str(totals["discount"]),
            tax_total=str(totals["tax"]),
            shipping_total=str(totals["shipping"]),
            net_sales_total=str(totals["net"]),
        )

    @staticmethod
    def _has_rows(df: DataFrame) -> bool:
        return bool(df.take(1))
