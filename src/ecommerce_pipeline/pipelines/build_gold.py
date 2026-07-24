from __future__ import annotations

import json
from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_commit_metadata,
    ensure_change_data_feed,
    latest_commit_metadata,
    latest_delta_version,
    set_delta_table_property,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.pipelines.quality import GoldQualityChecker
from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_category,
    build_dim_customer,
    build_dim_date,
    build_dim_location,
    build_dim_payment,
    build_dim_product,
    build_dim_promotion,
    build_dim_shipping,
    build_dim_shop,
    build_dim_time,
)
from ecommerce_pipeline.transformations.gold.fact_sales import build_fact_sales

GOLD_PIPELINE_NAME = "silver_to_gold"
FACT_SOURCE_TABLES = {
    "orders",
    "order_items",
    "order_vouchers",
    "payments",
    "shipments",
    "vouchers",
    "products",
}


class GoldBuilder:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)

    def run(self, *, batch_id: str = "standalone", full_rebuild: bool = False) -> list[str]:
        current_versions = self._silver_versions()
        previous_versions = self._read_progress()
        if full_rebuild or previous_versions is None:
            self._run_full()
            self._mark_progress(current_versions, batch_id)
            return self._paths()

        self._validate_progress(previous_versions, current_versions)
        changed_tables = {name for name, version in current_versions.items() if version > previous_versions[name]}
        if not changed_tables:
            return self._paths()

        changes = {
            name: self._read_changes(name, previous_versions[name] + 1, current_versions[name])
            for name in changed_tables
        }
        self._run_incremental(changes)
        self._mark_progress(current_versions, batch_id)
        return self._paths()

    def _run_full(self) -> None:
        tables = self._read_sources()
        customers = build_dim_customer(tables["app_users"], self.spark)
        self._merge_customers(customers)
        current_customers = self.lakehouse.read_table("gold", "dim_customer")
        outputs: dict[str, tuple[DataFrame, list[str]]] = {
            "dim_date": (build_dim_date(tables["orders"], self.spark), ["date_key"]),
            "dim_time": (build_dim_time(tables["orders"], self.spark), ["time_key"]),
            "dim_location": (
                build_dim_location(tables["user_addresses"], tables["orders"], self.spark),
                ["location_key"],
            ),
            "dim_shop": (build_dim_shop(tables["shops"], self.spark), ["shop_key"]),
            "dim_category": (build_dim_category(tables["categories"], self.spark), ["category_key"]),
            "dim_product": (
                build_dim_product(tables["products"], tables["product_variants"], self.spark),
                ["product_key"],
            ),
            "dim_promotion": (
                build_dim_promotion(tables["order_vouchers"], tables["vouchers"], self.spark),
                ["promotion_key"],
            ),
            "dim_payment": (build_dim_payment(tables["payments"], self.spark), ["payment_key"]),
            "dim_shipping": (build_dim_shipping(tables["shipments"], self.spark), ["shipping_key"]),
            "fact_sales": (build_fact_sales(tables, current_customers), ["source_order_id", "source_order_item_id"]),
        }
        for table_name, (df, keys) in outputs.items():
            self.lakehouse.upsert_table(df, "gold", table_name, keys, delete_not_matched_by_source=True)
        GoldQualityChecker(self.lakehouse).run(tables)

    def _run_incremental(self, changes: dict[str, DataFrame]) -> None:
        changed_tables = set(changes)
        if "app_users" in changes:
            user_ids = self._ids(changes["app_users"], "user_id")
            users = self._filter_current("app_users", "user_id", user_ids)
            self._merge_customers(build_dim_customer(users, self.spark))

        changed_order_ids = self._ids_if_changed(changes, "orders", "order_id")
        affected_order_ids = self._affected_order_ids(changes)

        if "orders" in changes:
            orders = self._filter_current("orders", "order_id", changed_order_ids)
            self._upsert_gold("dim_date", build_dim_date(orders, self.spark), ["date_key"])
            self._upsert_gold("dim_time", build_dim_time(orders, self.spark), ["time_key"])
        else:
            orders = self._empty_current("orders")

        if changed_tables & {"user_addresses", "orders"}:
            address_ids = self._ids_if_changed(changes, "user_addresses", "address_id")
            addresses = self._filter_current("user_addresses", "address_id", address_ids)
            self._upsert_gold(
                "dim_location",
                build_dim_location(addresses, orders, self.spark),
                ["location_key"],
            )

        if "shops" in changes:
            shop_ids = self._ids(changes["shops"], "shop_id")
            shops = self._filter_current("shops", "shop_id", shop_ids)
            self._upsert_gold("dim_shop", build_dim_shop(shops, self.spark), ["shop_key"])

        if "categories" in changes:
            category_ids = self._affected_category_ids(changes["categories"])
            categories = self._category_scope(category_ids)
            self._upsert_gold("dim_category", build_dim_category(categories, self.spark), ["category_key"])

        if changed_tables & {"products", "product_variants"}:
            product_ids = self._affected_product_ids(changes)
            products = self._filter_current("products", "product_id", product_ids)
            variants = self._filter_current("product_variants", "product_id", product_ids)
            self._upsert_gold(
                "dim_product",
                build_dim_product(products, variants, self.spark),
                ["product_key"],
            )

        if changed_tables & {"order_vouchers", "vouchers"}:
            order_vouchers, vouchers = self._promotion_scope(affected_order_ids)
            self._upsert_gold(
                "dim_promotion",
                build_dim_promotion(order_vouchers, vouchers, self.spark),
                ["promotion_key"],
            )

        if "payments" in changes:
            payment_ids = self._ids(changes["payments"], "payment_id")
            payments = self._filter_current("payments", "payment_id", payment_ids)
            self._upsert_gold("dim_payment", build_dim_payment(payments, self.spark), ["payment_key"])

        if "shipments" in changes:
            shipment_ids = self._ids(changes["shipments"], "shipment_id")
            shipments = self._filter_current("shipments", "shipment_id", shipment_ids)
            self._upsert_gold("dim_shipping", build_dim_shipping(shipments, self.spark), ["shipping_key"])

        if changed_tables & FACT_SOURCE_TABLES:
            fact_sources = self._fact_scope(affected_order_ids)
            fact_sources["orders"] = fact_sources["orders"].cache()
            fact_sources["order_items"] = fact_sources["order_items"].cache()
            customer_ids = fact_sources["orders"].select("customer_id").where("customer_id IS NOT NULL").distinct()
            customers = self._filter_gold("dim_customer", "source_customer_id", customer_ids, "customer_id")
            facts = build_fact_sales(fact_sources, customers).cache()
            fact_keys = ["source_order_id", "source_order_item_id"]
            try:
                existing_facts = self._filter_gold(
                    "fact_sales",
                    "source_order_id",
                    affected_order_ids,
                    "order_id",
                )
                stale_fact_keys = existing_facts.select(*fact_keys).join(
                    facts.select(*fact_keys),
                    fact_keys,
                    "left_anti",
                )
                self.lakehouse.delete_matching(
                    stale_fact_keys,
                    "gold",
                    "fact_sales",
                    fact_keys,
                )
                self.lakehouse.upsert_table(
                    facts,
                    "gold",
                    "fact_sales",
                    fact_keys,
                )
                GoldQualityChecker(self.lakehouse).run_incremental(facts, fact_sources)
            finally:
                facts.unpersist()
                fact_sources["orders"].unpersist()
                fact_sources["order_items"].unpersist()

    def _affected_order_ids(self, changes: dict[str, DataFrame]) -> DataFrame:
        frames = [
            self._ids(changes[name], "order_id")
            for name in ("orders", "order_items", "order_vouchers", "payments", "shipments")
            if name in changes
        ]
        if "vouchers" in changes:
            voucher_ids = self._ids(changes["vouchers"], "voucher_id")
            frames.append(self._filter_current("order_vouchers", "voucher_id", voucher_ids).select("order_id"))
            if "order_vouchers" in changes:
                frames.append(
                    changes["order_vouchers"]
                    .join(F.broadcast(voucher_ids), "voucher_id", "left_semi")
                    .select("order_id")
                )
        if "products" in changes:
            product_ids = self._ids(changes["products"], "product_id")
            frames.append(self._filter_current("order_items", "product_id", product_ids).select("order_id"))
        if not frames:
            return self._empty_current("orders").select("order_id")
        return self._union_ids(frames, "order_id")

    def _affected_category_ids(self, category_changes: DataFrame) -> DataFrame:
        changed = self._ids(category_changes, "category_id")
        children = self._filter_current("categories", "parent_category_id", changed, "category_id").select(
            "category_id"
        )
        return self._union_ids([changed, children], "category_id")

    def _category_scope(self, affected_ids: DataFrame) -> DataFrame:
        current = self.lakehouse.read_table("silver", "categories")
        affected = current.join(F.broadcast(affected_ids), "category_id", "left_semi")
        parent_ids = affected.select(F.col("parent_category_id").alias("category_id")).where("category_id IS NOT NULL")
        scope_ids = self._union_ids([affected_ids, parent_ids], "category_id")
        return current.join(F.broadcast(scope_ids), "category_id", "left_semi")

    def _affected_product_ids(self, changes: dict[str, DataFrame]) -> DataFrame:
        frames: list[DataFrame] = []
        if "products" in changes:
            frames.append(self._ids(changes["products"], "product_id"))
        if "product_variants" in changes:
            frames.append(self._ids(changes["product_variants"], "product_id"))
        return self._union_ids(frames, "product_id")

    def _promotion_scope(self, order_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
        order_vouchers = self._filter_current("order_vouchers", "order_id", order_ids)
        voucher_ids = order_vouchers.select("voucher_id").where("voucher_id IS NOT NULL").distinct()
        vouchers = self._filter_current("vouchers", "voucher_id", voucher_ids)
        return order_vouchers, vouchers

    def _fact_scope(self, order_ids: DataFrame) -> dict[str, DataFrame]:
        orders = self._filter_current("orders", "order_id", order_ids)
        items = self._filter_current("order_items", "order_id", order_ids)
        product_ids = items.select("product_id").where("product_id IS NOT NULL").distinct()
        products = self._filter_current("products", "product_id", product_ids)
        payments = self._filter_current("payments", "order_id", order_ids)
        shipments = self._filter_current("shipments", "order_id", order_ids)
        order_vouchers, vouchers = self._promotion_scope(order_ids)
        return {
            "orders": orders,
            "order_items": items,
            "products": products,
            "payments": payments,
            "shipments": shipments,
            "order_vouchers": order_vouchers,
            "vouchers": vouchers,
        }

    def _silver_versions(self) -> dict[str, int]:
        versions: dict[str, int] = {}
        for table_name in SILVER_TABLES:
            path = self.config.lakehouse.table_path("silver", table_name)
            if not self.lakehouse.table_exists("silver", table_name):
                raise RuntimeError(f"Silver Delta table is missing for table: {table_name}")
            ensure_change_data_feed(self.spark, path)
            versions[table_name] = latest_delta_version(self.spark, path)
        return versions

    def _read_progress(self) -> dict[str, int] | None:
        if not self.lakehouse.table_exists("gold", "fact_sales"):
            return None
        metadata = latest_commit_metadata(
            self.spark,
            self.config.lakehouse.table_path("gold", "fact_sales"),
            pipeline=GOLD_PIPELINE_NAME,
        )
        if metadata is None:
            return None
        raw_versions = metadata.get("silver_versions")
        if not isinstance(raw_versions, dict):
            raise ValueError("Invalid Gold Delta progress metadata: silver_versions")
        versions: dict[str, int] = {}
        for name, value in raw_versions.items():
            if not isinstance(name, str) or not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError("Invalid Gold Delta progress metadata: silver_versions")
            versions[name] = value
        return versions

    @staticmethod
    def _validate_progress(previous: dict[str, int], current: dict[str, int]) -> None:
        if set(previous) != set(current):
            raise RuntimeError("Gold progress does not match Silver contracts; run with --full-rebuild-gold")
        ahead = {name: previous[name] for name in current if previous[name] > current[name]}
        if ahead:
            raise RuntimeError(f"Gold progress is ahead of Silver: {ahead}")

    def _mark_progress(self, versions: dict[str, int], batch_id: str) -> None:
        fact_path = self.config.lakehouse.table_path("gold", "fact_sales")
        metadata: dict[str, object] = {
            "pipeline": GOLD_PIPELINE_NAME,
            "batch_id": batch_id,
            "silver_versions": versions,
        }
        with delta_commit_metadata(self.spark, metadata):
            set_delta_table_property(
                self.spark,
                fact_path,
                "pipeline.goldProgressMarker",
                json.dumps(versions, separators=(",", ":"), sort_keys=True),
            )

    def _read_changes(self, table_name: str, starting_version: int, ending_version: int) -> DataFrame:
        return (
            self.spark.read.format(self.config.lakehouse.format)
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .option("endingVersion", ending_version)
            .load(self.config.lakehouse.table_path("silver", table_name))
        )

    def _read_sources(self) -> dict[str, DataFrame]:
        return {name: self.lakehouse.read_table("silver", name) for name in SILVER_TABLES}

    def _empty_current(self, table_name: str) -> DataFrame:
        return self.lakehouse.read_table("silver", table_name).limit(0)

    def _filter_current(
        self,
        table_name: str,
        table_column: str,
        ids: DataFrame,
        id_column: str | None = None,
    ) -> DataFrame:
        id_name = id_column or table_column
        key_set = ids.where(F.col(id_name).isNotNull()).select(F.col(id_name).alias(table_column)).distinct()
        return self.lakehouse.read_table("silver", table_name).join(
            F.broadcast(key_set),
            table_column,
            "left_semi",
        )

    def _filter_gold(
        self,
        table_name: str,
        table_column: str,
        ids: DataFrame,
        id_column: str | None = None,
    ) -> DataFrame:
        id_name = id_column or table_column
        key_set = ids.where(F.col(id_name).isNotNull()).select(F.col(id_name).alias(table_column)).distinct()
        return self.lakehouse.read_table("gold", table_name).join(
            F.broadcast(key_set),
            table_column,
            "left_semi",
        )

    @staticmethod
    def _ids(df: DataFrame, column: str) -> DataFrame:
        return df.select(column).where(F.col(column).isNotNull()).distinct()

    def _ids_if_changed(self, changes: dict[str, DataFrame], table_name: str, column: str) -> DataFrame:
        if table_name in changes:
            return self._ids(changes[table_name], column)
        return self._empty_current(table_name).select(column)

    @staticmethod
    def _union_ids(frames: Iterable[DataFrame], column: str) -> DataFrame:
        iterator = iter(frames)
        result = next(iterator).select(column)
        for frame in iterator:
            result = result.unionByName(frame.select(column))
        return result.where(F.col(column).isNotNull()).distinct()

    def _merge_customers(self, customers: DataFrame) -> None:
        self.lakehouse.merge_scd2(
            customers,
            "gold",
            "dim_customer",
            "source_customer_id",
            "customer_key",
            "attribute_hash",
            type1_columns=("last_login_at",),
        )

    def _upsert_gold(self, table_name: str, df: DataFrame, keys: list[str]) -> None:
        self.lakehouse.upsert_table(df, "gold", table_name, keys)

    def _paths(self) -> list[str]:
        names = [
            "dim_customer",
            "dim_date",
            "dim_time",
            "dim_location",
            "dim_shop",
            "dim_category",
            "dim_product",
            "dim_promotion",
            "dim_payment",
            "dim_shipping",
            "fact_sales",
        ]
        return [self.config.lakehouse.table_path("gold", name) for name in names]


def build_gold(
    spark: SparkSession,
    config: AppConfig,
    *,
    batch_id: str = "standalone",
    full_rebuild: bool = False,
) -> list[str]:
    return GoldBuilder(spark, config).run(batch_id=batch_id, full_rebuild=full_rebuild)
