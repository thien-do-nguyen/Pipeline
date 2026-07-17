from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.batch.quality import GoldQualityChecker
from ecommerce_pipeline.config.models import AppConfig
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
    read_silver,
)
from ecommerce_pipeline.transformations.gold.fact_sales import build_fact_sales


class GoldBuilder:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)

    def run(self) -> list[str]:
        tables = self._read_sources()
        customers = build_dim_customer(tables["app_users"], self.spark)
        self.lakehouse.merge_scd2(
            customers,
            "gold",
            "dim_customer",
            "source_customer_id",
            "customer_key",
            "attribute_hash",
            type1_columns=("last_login_at",),
        )
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
            self.lakehouse.upsert_table(df, "gold", table_name, keys)
        GoldQualityChecker(self.lakehouse).run(tables)
        return [
            self.config.lakehouse.table_path("gold", "dim_customer"),
            *[self.config.lakehouse.table_path("gold", name) for name in outputs],
        ]

    def _read_sources(self) -> dict[str, DataFrame]:
        names = [
            "app_users",
            "user_addresses",
            "shops",
            "categories",
            "products",
            "product_variants",
            "vouchers",
            "orders",
            "order_items",
            "order_vouchers",
            "payments",
            "shipments",
        ]
        return {name: read_silver(self.spark, self.config, name) for name in names}


def build_gold(spark: SparkSession, config: AppConfig) -> list[str]:
    return GoldBuilder(spark, config).run()
