from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.batch.bronze_utils import (
    add_bronze_metadata,
    bronze_batch_path,
    delta_table_exists,
    drop_already_appended,
    max_watermark,
    read_batch_upper_bound,
    source_select_query,
    validate_source_frame,
    write_append_only,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.bronze_source_tables import BRONZE_SOURCE_TABLES, BronzeTableContract
from ecommerce_pipeline.control.batch_runs import (
    BronzeTableResult,
    CompositeWatermark,
    read_bronze_watermark,
    write_bronze_watermark,
)


class BronzeExtractor:
    def __init__(self, spark: SparkSession, config: AppConfig, batch_id: str) -> None:
        self.spark = spark
        self.config = config
        self.batch_id = batch_id
        self.reader = PostgresReader(spark, config)

    def run_table(self, table_name: str) -> BronzeTableResult:
        contract = self._get_contract(table_name)
        output_path = bronze_batch_path(self.config, table_name)
        table_exists = delta_table_exists(self.spark, output_path)
        previous_watermark = self.read_watermark(table_name, table_exists)
        batch_upper_bound = read_batch_upper_bound(self.reader, self.config, contract)
        source_df = self.read_source(contract, previous_watermark, batch_upper_bound).cache()
        write_df: DataFrame | None = None

        try:
            validate_source_frame(source_df, contract)
            current_watermark = max_watermark(source_df, contract)
            write_df = self._deduplicate_before_append(source_df, output_path, table_exists, contract)
            record_count = write_df.count()
            self.append_bronze(write_df, output_path, table_exists, record_count)
        finally:
            if table_exists and write_df is not None:
                write_df.unpersist()
            source_df.unpersist()

        final_watermark = current_watermark or previous_watermark
        self.update_watermark(table_name, final_watermark)
        return self._result(
            table_name=table_name,
            output_path=output_path,
            record_count=record_count,
            ingestion_type="incremental" if previous_watermark else "full",
            batch_upper_bound=batch_upper_bound,
            previous_watermark=previous_watermark,
            current_watermark=final_watermark,
        )

    def read_source(
        self,
        contract: BronzeTableContract,
        previous_watermark: CompositeWatermark | None,
        batch_upper_bound: str | None,
    ) -> DataFrame:
        query = source_select_query(self.config, contract, previous_watermark, batch_upper_bound)
        raw_df = self.reader.read_query(query)
        return add_bronze_metadata(raw_df, self.config, contract.table_name, self.batch_id)

    def append_bronze(self, df: DataFrame, output_path: str, table_exists: bool, record_count: int) -> None:
        if record_count > 0 or not table_exists:
            write_append_only(df, self.config, output_path, table_exists)

    def update_watermark(self, table_name: str, watermark: CompositeWatermark | None) -> None:
        write_bronze_watermark(
            self.config.lakehouse.base_path,
            table_name,
            self.batch_id,
            watermark,
            self.config.application.timezone,
            self.spark,
        )

    def read_watermark(self, table_name: str, table_exists: bool) -> CompositeWatermark | None:
        if not table_exists:
            return None
        return read_bronze_watermark(self.config.lakehouse.base_path, table_name, self.spark)

    def _deduplicate_before_append(
        self,
        source_df: DataFrame,
        output_path: str,
        table_exists: bool,
        contract: BronzeTableContract,
    ) -> DataFrame:
        if not table_exists:
            return source_df
        return drop_already_appended(self.spark, source_df, output_path, contract).cache()

    def _get_contract(self, table_name: str) -> BronzeTableContract:
        contract = BRONZE_SOURCE_TABLES.get(table_name)
        if contract is None:
            raise ValueError(f"Unknown source table: {table_name}")
        return contract

    def _result(
        self,
        table_name: str,
        output_path: str,
        record_count: int,
        ingestion_type: str,
        batch_upper_bound: str | None,
        previous_watermark: CompositeWatermark | None,
        current_watermark: CompositeWatermark | None,
    ) -> BronzeTableResult:
        return BronzeTableResult(
            batch_id=self.batch_id,
            table_name=table_name,
            output_path=output_path,
            record_count=record_count,
            ingestion_type=ingestion_type,
            batch_upper_bound=batch_upper_bound,
            previous_watermark_value=previous_watermark.column_value if previous_watermark else None,
            previous_watermark_pk=previous_watermark.primary_key_value if previous_watermark else None,
            current_watermark_value=current_watermark.column_value if current_watermark else None,
            current_watermark_pk=current_watermark.primary_key_value if current_watermark else None,
        )


def extract_table_to_bronze(
    spark: SparkSession,
    config: AppConfig,
    table_name: str,
    batch_id: str,
) -> BronzeTableResult:
    return BronzeExtractor(spark, config, batch_id).run_table(table_name)


def extract_all_to_bronze(
    spark: SparkSession,
    config: AppConfig,
    batch_id: str,
    table_names: list[str] | None = None,
) -> list[BronzeTableResult]:
    tables = list(BRONZE_SOURCE_TABLES) if table_names is None else table_names
    unknown = sorted(set(tables) - set(BRONZE_SOURCE_TABLES))
    if unknown:
        raise ValueError(f"Unknown source tables: {unknown}")
    extractor = BronzeExtractor(spark, config, batch_id)
    return [extractor.run_table(table_name) for table_name in tables]
