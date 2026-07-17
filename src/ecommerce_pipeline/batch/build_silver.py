from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.batch.bronze_utils import bronze_batch_path
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_source_tables import SOURCE_TABLES, SilverTableContract
from ecommerce_pipeline.transformations.silver.common import SILVER_SCHEMA_VERSION
from ecommerce_pipeline.transformations.silver.customers import supports_customer_table, transform_customer_table
from ecommerce_pipeline.transformations.silver.sales import supports_sales_table, transform_sales_table


class SilverBuilder:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)

    def run(self, table_names: list[str] | None = None) -> list[str]:
        tables = list(SOURCE_TABLES) if table_names is None else table_names
        unknown = sorted(set(tables) - set(SOURCE_TABLES))
        if unknown:
            raise ValueError(f"Unknown silver source tables: {unknown}")
        return [self.run_table(table_name) for table_name in tables]

    def run_table(self, table_name: str) -> str:
        contract = self._get_contract(table_name)
        df = self.transform(contract)
        if self._requires_rebuild(table_name):
            self.lakehouse.write_table(df, "silver", table_name)
            return self.silver_path(table_name)
        self.lakehouse.upsert_table(
            df,
            "silver",
            table_name,
            contract.primary_keys,
        )
        return self.silver_path(table_name)

    def transform(self, contract: SilverTableContract) -> DataFrame:
        bronze_df = self.read_changes(contract.table_name)
        if supports_customer_table(contract.table_name):
            return transform_customer_table(bronze_df, contract)
        if supports_sales_table(contract.table_name):
            return transform_sales_table(bronze_df, contract)
        raise ValueError(f"Missing silver transformation for table: {contract.table_name}")

    def read_changes(self, table_name: str) -> DataFrame:
        # Local data volumes are deliberately small. Reading the append-only
        # Bronze history and selecting the latest source version is deterministic
        # and avoids coupling correctness to lexicographic batch-id ordering.
        bronze = self.spark.read.format(self.config.lakehouse.format).load(bronze_batch_path(self.config, table_name))
        if "_operation" not in bronze.columns:
            bronze = bronze.withColumn("_operation", F.lit("UPSERT"))
        return bronze

    def _requires_rebuild(self, table_name: str) -> bool:
        if not self.lakehouse.table_exists("silver", table_name):
            return False
        silver = self.lakehouse.read_table("silver", table_name)
        if "_silver_schema_version" not in silver.columns:
            return True
        row = silver.agg(F.max("_silver_schema_version").alias("version")).first()
        return row is None or row["version"] != SILVER_SCHEMA_VERSION

    def silver_path(self, table_name: str) -> str:
        return self.config.lakehouse.table_path("silver", table_name)

    def _get_contract(self, table_name: str) -> SilverTableContract:
        contract = SOURCE_TABLES.get(table_name)
        if contract is None:
            raise ValueError(f"Unknown silver source table: {table_name}")
        return contract


def build_silver(spark: SparkSession, config: AppConfig, table_names: list[str] | None = None) -> list[str]:
    return SilverBuilder(spark, config).run(table_names)
