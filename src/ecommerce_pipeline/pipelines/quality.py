from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
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


@dataclass(frozen=True)
class _FactMetrics:
    rows: int
    distinct_source_keys: int
    distinct_sales_keys: int
    invalid_amount_rows: int
    unresolved_key_rows: int
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
    MONETARY_COLUMNS = (
        "unit_price_amount",
        "gross_sales_amount",
        "line_discount_amount",
        "order_discount_amount_allocated",
        "total_discount_amount",
        "tax_amount",
        "shipping_amount_allocated",
        "net_sales_amount",
    )

    def __init__(self, lakehouse: LakehouseAdapter) -> None:
        self.lakehouse = lakehouse

    def run(self, sources: Mapping[str, DataFrame] | None = None) -> GoldQualityReport:
        fact = self.lakehouse.read_table("gold", "fact_sales")
        if sources is None:
            sources = {
                "orders": self.lakehouse.read_table("silver", "orders"),
                "order_items": self.lakehouse.read_table("silver", "order_items"),
            }
        orders = sources["orders"]
        items = sources["order_items"]
        with self._cached_for_quality(fact, orders, items):
            metrics = self._collect_fact_metrics(fact)
            self._assert_fact_metrics(metrics, "fact_sales")
            self._assert_dimension_integrity(fact, check_uniqueness=True)
            self._assert_customer_scd2()
            return self._assert_reconciliation(fact, orders, items, metrics)

    def run_incremental(self, fact: DataFrame, sources: Mapping[str, DataFrame]) -> GoldQualityReport:
        """Validate only the bounded fact rows rebuilt by the current micro-batch."""

        orders = sources["orders"]
        items = sources["order_items"]
        with self._cached_for_quality(fact, orders, items):
            metrics = self._collect_fact_metrics(fact)
            self._assert_fact_metrics(metrics, "fact_sales incremental")
            self._assert_dimension_integrity(fact, check_uniqueness=False)
            return self._assert_reconciliation(fact, orders, items, metrics)

    @staticmethod
    @contextmanager
    def _cached_for_quality(*dataframes: DataFrame) -> Iterator[None]:
        """Cache only uncached inputs and release only cache entries owned here."""

        owned: list[DataFrame] = []
        for dataframe in dataframes:
            if not dataframe.is_cached:
                dataframe.cache()
                owned.append(dataframe)
        try:
            yield
        finally:
            for dataframe in owned:
                dataframe.unpersist()

    def _collect_fact_metrics(self, fact: DataFrame) -> _FactMetrics:
        invalid_amount = F.col("quantity").isNull() | (F.col("quantity") <= 0)
        for name in self.MONETARY_COLUMNS:
            invalid_amount = invalid_amount | F.col(name).isNull() | (F.col(name) < 0)

        unresolved_key = F.lit(False)
        for key in self.REQUIRED_NON_UNKNOWN_KEYS:
            unresolved_key = unresolved_key | F.col(key).isNull() | (F.col(key) == 0)

        row = fact.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(F.struct("source_order_id", "source_order_item_id")).alias("distinct_source_keys"),
            F.countDistinct(F.struct("sales_key")).alias("distinct_sales_keys"),
            F.sum(invalid_amount.cast("long")).alias("invalid_amount_rows"),
            F.sum(unresolved_key.cast("long")).alias("unresolved_key_rows"),
            F.sum("gross_sales_amount").alias("gross"),
            F.sum("total_discount_amount").alias("discount"),
            F.sum("tax_amount").alias("tax"),
            F.sum("shipping_amount_allocated").alias("shipping"),
            F.sum("net_sales_amount").alias("net"),
        ).first()
        if row is None:
            raise ValueError("Gold fact metrics are unavailable")
        return _FactMetrics(
            rows=int(row["rows"]),
            distinct_source_keys=int(row["distinct_source_keys"]),
            distinct_sales_keys=int(row["distinct_sales_keys"]),
            invalid_amount_rows=int(row["invalid_amount_rows"] or 0),
            unresolved_key_rows=int(row["unresolved_key_rows"] or 0),
            gross_sales_total=str(row["gross"]),
            discount_total=str(row["discount"]),
            tax_total=str(row["tax"]),
            shipping_total=str(row["shipping"]),
            net_sales_total=str(row["net"]),
        )

    @staticmethod
    def _assert_fact_metrics(metrics: _FactMetrics, table_name: str) -> None:
        if metrics.distinct_source_keys != metrics.rows:
            raise ValueError(f"Duplicate key in gold.{table_name}: ['source_order_id', 'source_order_item_id']")
        if metrics.distinct_sales_keys != metrics.rows:
            raise ValueError(f"Duplicate key in gold.{table_name}: ['sales_key']")
        if metrics.invalid_amount_rows:
            raise ValueError("Gold fact contains null/invalid quantity or monetary amounts")
        if metrics.unresolved_key_rows:
            raise ValueError("Gold fact contains unresolved required dimension keys")

    def _assert_dimension_integrity(self, fact: DataFrame, *, check_uniqueness: bool) -> None:
        dimensions = {
            name: self.lakehouse.read_table("gold", name).select(key) for name, (key, _) in self.DIMENSION_KEYS.items()
        }
        dimensions["dim_location"] = self.lakehouse.read_table("gold", "dim_location").select("location_key")

        violations: list[DataFrame] = []
        if check_uniqueness:
            for name, keys in dimensions.items():
                key = keys.columns[0]
                violations.append(
                    keys.groupBy(key)
                    .count()
                    .filter(F.col("count") > 1)
                    .select(F.lit(f"Duplicate key in gold.{name}: ['{key}']").alias("violation"))
                )

        for dimension, (dimension_key, fact_key) in self.DIMENSION_KEYS.items():
            violations.append(self._missing_reference(fact, dimensions[dimension], dimension, dimension_key, fact_key))
        for fact_key in ("ship_to_location_key", "bill_to_location_key"):
            violations.append(
                self._missing_reference(fact, dimensions["dim_location"], "dim_location", "location_key", fact_key)
            )

        violation = self._union_all(violations).first()
        if violation is not None:
            raise ValueError(str(violation["violation"]))

    @staticmethod
    def _missing_reference(
        fact: DataFrame,
        dimension_keys: DataFrame,
        dimension: str,
        dimension_key: str,
        fact_key: str,
    ) -> DataFrame:
        references = dimension_keys.select(F.col(dimension_key).alias(fact_key)).distinct()
        return (
            fact.select(fact_key)
            .distinct()
            .join(references, fact_key, "left_anti")
            .select(F.lit(f"Missing gold.{dimension} reference for {fact_key}").alias("violation"))
        )

    def _assert_customer_scd2(self) -> None:
        customers = self.lakehouse.read_table("gold", "dim_customer").filter(F.col("source_customer_id").isNotNull())
        all_members = customers.select("source_customer_id").distinct()
        current_counts = customers.filter("is_current").groupBy("source_customer_id").count()
        invalid_current = (
            all_members.join(current_counts, "source_customer_id", "left")
            .filter(F.coalesce(F.col("count"), F.lit(0)) != 1)
            .select(F.lit("invalid current member count").alias("violation"))
        )
        invalid_range = customers.filter(F.col("effective_from") >= F.col("effective_to")).select(
            F.lit("invalid effective date range").alias("violation")
        )
        history = Window.partitionBy("source_customer_id").orderBy("effective_from", "customer_key")
        overlap = (
            customers.withColumn("_previous_to", F.lag("effective_to").over(history))
            .filter(F.col("_previous_to").isNotNull() & (F.col("effective_from") < F.col("_previous_to")))
            .select(F.lit("overlapping effective date range").alias("violation"))
        )
        violation = self._union_all([invalid_current, invalid_range, overlap]).first()
        if violation is not None:
            raise ValueError(f"Invalid SCD2 history in gold.dim_customer: {violation['violation']}")

    def _assert_reconciliation(
        self,
        fact: DataFrame,
        orders: DataFrame,
        items: DataFrame,
        metrics: _FactMetrics,
    ) -> GoldQualityReport:
        counts = (
            orders.agg(F.count(F.lit(1)).alias("source_order_rows"))
            .crossJoin(items.agg(F.count(F.lit(1)).alias("source_item_rows")))
            .first()
        )
        if counts is None:
            raise ValueError("Source row counts are unavailable")
        source_order_rows = int(counts["source_order_rows"])
        source_item_rows = int(counts["source_item_rows"])
        if source_item_rows != metrics.rows:
            raise ValueError(f"Fact row count mismatch: source_items={source_item_rows}, fact_rows={metrics.rows}")

        source_keys = items.select(
            F.col("order_id").alias("source_order_id"),
            F.col("order_item_id").alias("source_order_item_id"),
        )
        fact_keys = fact.select("source_order_id", "source_order_item_id")
        missing_items = source_keys.join(fact_keys, ["source_order_id", "source_order_item_id"], "left_anti").select(
            F.lit("missing_order_item").alias("violation"),
            F.to_json(F.struct("source_order_id", "source_order_item_id")).alias("details"),
        )

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
        tolerance = F.lit("0.01").cast("decimal(18,2)")
        monetary_mismatches = (
            expected.join(actual, "source_order_id", "left")
            .filter(
                F.col("actual_net").isNull()
                | (F.abs(F.col("expected_gross") - F.col("actual_gross")) > tolerance)
                | (F.abs(F.col("expected_discount") - F.col("actual_discount")) > tolerance)
                | (F.abs(F.col("expected_tax") - F.col("actual_tax")) > tolerance)
                | (F.abs(F.col("expected_shipping") - F.col("actual_shipping")) > tolerance)
                | (F.abs(F.col("expected_net") - F.col("actual_net")) > tolerance)
            )
            .select(
                F.lit("monetary_mismatch").alias("violation"),
                F.to_json(F.struct("*")).alias("details"),
            )
        )
        violations = self._union_all([missing_items, monetary_mismatches]).limit(3).collect()
        if violations:
            if any(row["violation"] == "missing_order_item" for row in violations):
                raise ValueError("Gold fact is missing one or more source order-item keys")
            examples = [row["details"] for row in violations]
            raise ValueError(f"Source-to-Gold monetary reconciliation failed: {examples}")

        return GoldQualityReport(
            source_order_rows=source_order_rows,
            source_item_rows=source_item_rows,
            fact_rows=metrics.rows,
            gross_sales_total=metrics.gross_sales_total,
            discount_total=metrics.discount_total,
            tax_total=metrics.tax_total,
            shipping_total=metrics.shipping_total,
            net_sales_total=metrics.net_sales_total,
        )

    @staticmethod
    def _union_all(dataframes: Sequence[DataFrame]) -> DataFrame:
        if not dataframes:
            raise ValueError("At least one quality rule is required")
        result = dataframes[0]
        for dataframe in dataframes[1:]:
            result = result.unionByName(dataframe)
        return result
