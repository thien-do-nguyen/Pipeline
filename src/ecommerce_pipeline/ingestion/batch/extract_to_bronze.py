from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import (
    delta_commit_metadata,
    ensure_change_data_feed,
    latest_commit_metadata,
    set_delta_table_property,
)
from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES, BronzeTableContract, get_bronze_contract
from ecommerce_pipeline.contracts.change_events import EventCursor
from ecommerce_pipeline.control.batch_runs import BronzeTableResult
from ecommerce_pipeline.ingestion.batch.bronze_operations import (
    add_bronze_metadata,
    bronze_batch_path,
    change_event_query,
    delta_table_exists,
    jdbc_partition_count,
    max_event_cursor,
    read_batch_upper_bound,
    validate_change_events,
    write_append_only,
)


class BronzeExtractor:
    def __init__(self, spark: SparkSession, config: AppConfig, batch_id: str) -> None:
        self.spark = spark
        self.config = config
        self.batch_id = batch_id
        self.reader = PostgresReader(spark, config)

    def run_table(self, table_name: str) -> BronzeTableResult:
        contract = get_bronze_contract(table_name)
        output_path = bronze_batch_path(self.config, table_name)
        table_exists = delta_table_exists(self.spark, output_path)
        previous = self._read_cursor(table_name, output_path, table_exists)
        upper_bound = read_batch_upper_bound(self.reader, self.config, contract, previous)
        source_df, record_count = self._materialize_events(contract, previous, upper_bound)

        try:
            validate_change_events(source_df, contract)
            current = max_event_cursor(source_df) or previous or EventCursor(0)
            if record_count > 0 or not table_exists:
                write_append_only(
                    self.spark,
                    source_df,
                    self.config,
                    output_path,
                    table_exists,
                    table_name=table_name,
                    batch_id=self.batch_id,
                    cursor=current,
                )
        finally:
            source_df.unpersist()

        return BronzeTableResult(
            batch_id=self.batch_id,
            table_name=table_name,
            output_path=output_path,
            record_count=record_count,
            ingestion_type="incremental" if table_exists else "initial",
            batch_upper_bound_event_id=upper_bound,
            previous_event_id=previous.last_event_id if previous else None,
            current_event_id=current.last_event_id if current else None,
        )

    def _read_events(
        self,
        contract: BronzeTableContract,
        cursor: EventCursor | None,
        upper_bound: int | None,
    ) -> DataFrame:
        query = change_event_query(self.config, contract, cursor, upper_bound)
        if upper_bound is None:
            raw_df = self.reader.read_query(query)
        else:
            lower_bound = cursor.last_event_id if cursor else 0
            raw_df = self.reader.read_partitioned_query(
                query,
                partition_column="_event_id",
                lower_bound=lower_bound + 1,
                upper_bound=upper_bound + 1,
                num_partitions=jdbc_partition_count(self.config, cursor, upper_bound),
            )
        if contract.excluded_columns:
            raw_df = raw_df.drop(*contract.excluded_columns)
        return add_bronze_metadata(raw_df, self.config, contract.table_name, self.batch_id)

    def _materialize_events(
        self,
        contract: BronzeTableContract,
        cursor: EventCursor | None,
        upper_bound: int | None,
    ) -> tuple[DataFrame, int]:
        def materialize() -> tuple[DataFrame, int]:
            source_df = self._read_events(contract, cursor, upper_bound).cache()
            try:
                return source_df, source_df.count()
            except Exception:
                source_df.unpersist()
                raise

        return self.reader.with_retry(materialize, operation=f"JDBC extraction for {contract.table_name}")

    def _read_cursor(self, table_name: str, output_path: str, table_exists: bool) -> EventCursor | None:
        if not table_exists:
            return None
        ensure_change_data_feed(self.spark, output_path)
        metadata = latest_commit_metadata(self.spark, output_path, pipeline="postgres_to_bronze")
        if metadata is None:
            return self._migrate_legacy_cursor(table_name, output_path)
        last_event_id = metadata.get("last_event_id")
        if not isinstance(last_event_id, int) or isinstance(last_event_id, bool) or last_event_id < 0:
            raise ValueError(f"Invalid Bronze Delta progress metadata for table: {table_name}")
        return EventCursor(last_event_id)

    def _migrate_legacy_cursor(self, table_name: str, output_path: str) -> EventCursor:
        bronze = self.spark.read.format(self.config.lakehouse.format).load(output_path)
        if "_event_id" not in bronze.columns:
            raise RuntimeError(
                f"Bronze schema is outdated for {table_name}; rebuild Bronze before change-event ingestion"
            )
        row = bronze.agg(F.max("_event_id").alias("last_event_id")).first()
        cursor = EventCursor(0 if row is None or row["last_event_id"] is None else int(row["last_event_id"]))
        metadata: dict[str, object] = {
            "pipeline": "postgres_to_bronze",
            "source_table": table_name,
            "batch_id": self.batch_id,
            "last_event_id": cursor.last_event_id,
            "migration": "legacy_bronze_cursor",
        }
        with delta_commit_metadata(self.spark, metadata):
            set_delta_table_property(
                self.spark,
                output_path,
                "pipeline.bronzeProgressMigration",
                self.batch_id,
            )
        return cursor


def extract_all_to_bronze(
    spark: SparkSession,
    config: AppConfig,
    batch_id: str,
    table_names: list[str] | None = None,
) -> list[BronzeTableResult]:
    tables = list(BRONZE_TABLES) if table_names is None else table_names
    unknown = sorted(set(tables) - set(BRONZE_TABLES))
    if unknown:
        raise ValueError(f"Unknown source tables: {unknown}")
    extractor = BronzeExtractor(spark, config, batch_id)
    return [extractor.run_table(table_name) for table_name in tables]
