from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_commit_metadata,
    delta_table_state,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES, SilverTableContract, get_silver_contract
from ecommerce_pipeline.ingestion.batch.bronze_operations import bronze_batch_path
from ecommerce_pipeline.transformations.silver.common import SILVER_SCHEMA_VERSION
from ecommerce_pipeline.transformations.silver.customers import supports_customer_table, transform_customer_table
from ecommerce_pipeline.transformations.silver.sales import supports_sales_table, transform_sales_table

SILVER_PIPELINE_NAME = "bronze_to_silver"


class SilverBuilder:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)

    def run(
        self,
        table_names: list[str] | None = None,
        *,
        batch_id: str,
        full_rebuild: bool = False,
    ) -> list[str]:
        tables = list(SILVER_TABLES) if table_names is None else table_names
        unknown = sorted(set(tables) - set(SILVER_TABLES))
        if unknown:
            raise ValueError(f"Unknown silver source tables: {unknown}")
        return [self.run_table(table_name, batch_id=batch_id, full_rebuild=full_rebuild) for table_name in tables]

    def run_table(self, table_name: str, *, batch_id: str, full_rebuild: bool = False) -> str:
        contract = get_silver_contract(table_name)
        bronze_path = bronze_batch_path(self.config, table_name)
        bronze_state = delta_table_state(self.spark, bronze_path, pipeline="postgres_to_bronze")
        current_bronze_version = (
            bronze_state.version if bronze_state.progress_version is None else bronze_state.progress_version
        )
        table_exists = self.lakehouse.table_exists("silver", table_name)

        if full_rebuild or not table_exists:
            self._replace_from_snapshot(contract, current_bronze_version, batch_id)
            return self.silver_path(table_name)

        silver_state = delta_table_state(
            self.spark,
            self.silver_path(table_name),
            pipeline=SILVER_PIPELINE_NAME,
        )
        previous_bronze_version = self._processed_version(table_name, silver_state.progress)
        if previous_bronze_version is None:
            self._replace_from_snapshot(contract, current_bronze_version, batch_id)
            return self.silver_path(table_name)
        if previous_bronze_version > current_bronze_version:
            raise RuntimeError(
                f"Silver progress is ahead of Bronze for {table_name}: "
                f"silver_version={previous_bronze_version}, bronze_version={current_bronze_version}"
            )
        if previous_bronze_version == current_bronze_version:
            return self.silver_path(table_name)

        self._validate_schema_version(table_name)
        changes = self.read_changes(
            table_name,
            starting_version=previous_bronze_version + 1,
            ending_version=current_bronze_version,
        )
        transformed = self.transform(contract, changes)
        with delta_commit_metadata(
            self.spark,
            self._progress_metadata(table_name, current_bronze_version, batch_id),
        ):
            self.lakehouse.upsert_table(
                transformed,
                "silver",
                table_name,
                contract.primary_keys,
                delete_mode="hard",
            )
        return self.silver_path(table_name)

    def transform(self, contract: SilverTableContract, bronze_df: DataFrame) -> DataFrame:
        if supports_customer_table(contract.table_name):
            return transform_customer_table(bronze_df, contract)
        if supports_sales_table(contract.table_name):
            return transform_sales_table(bronze_df, contract)
        raise ValueError(f"Missing silver transformation for table: {contract.table_name}")

    def read_snapshot(self, table_name: str) -> DataFrame:
        return self.spark.read.format(self.config.lakehouse.format).load(bronze_batch_path(self.config, table_name))

    def read_changes(self, table_name: str, *, starting_version: int, ending_version: int) -> DataFrame:
        return (
            self.spark.read.format(self.config.lakehouse.format)
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .option("endingVersion", ending_version)
            .load(bronze_batch_path(self.config, table_name))
            .where("_change_type = 'insert'")
            .drop("_change_type", "_commit_version", "_commit_timestamp")
        )

    @staticmethod
    def _processed_version(table_name: str, metadata: dict[str, object] | None) -> int | None:
        if metadata is None:
            return None
        value = metadata.get("last_processed_bronze_version")
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"Invalid Silver Delta progress for table: {table_name}")
        return value

    def _replace_from_snapshot(
        self,
        contract: SilverTableContract,
        bronze_version: int,
        batch_id: str,
    ) -> None:
        snapshot = self.transform(contract, self.read_snapshot(contract.table_name)).filter(
            F.coalesce(F.col("_operation") != F.lit("DELETE"), F.lit(True))
        )
        with delta_commit_metadata(
            self.spark,
            self._progress_metadata(contract.table_name, bronze_version, batch_id),
        ):
            self.lakehouse.write_table(
                snapshot,
                "silver",
                contract.table_name,
                enable_change_data_feed=True,
            )

    @staticmethod
    def _progress_metadata(table_name: str, bronze_version: int, batch_id: str) -> dict[str, object]:
        return {
            "pipeline": SILVER_PIPELINE_NAME,
            "source_table": table_name,
            "batch_id": batch_id,
            "last_processed_bronze_version": bronze_version,
        }

    def _validate_schema_version(self, table_name: str) -> None:
        silver = self.lakehouse.read_table("silver", table_name)
        if "_silver_schema_version" not in silver.columns:
            raise RuntimeError(f"Silver schema metadata is missing for {table_name}; run with --full-rebuild-silver")
        row = silver.select("_silver_schema_version").limit(1).first()
        if row is None or row["_silver_schema_version"] != SILVER_SCHEMA_VERSION:
            raise RuntimeError(f"Silver schema version is outdated for {table_name}; run with --full-rebuild-silver")

    def silver_path(self, table_name: str) -> str:
        return self.config.lakehouse.table_path("silver", table_name)


def build_silver(
    spark: SparkSession,
    config: AppConfig,
    table_names: list[str] | None = None,
    *,
    batch_id: str,
    full_rebuild: bool = False,
) -> list[str]:
    return SilverBuilder(spark, config).run(
        table_names,
        batch_id=batch_id,
        full_rebuild=full_rebuild,
    )
