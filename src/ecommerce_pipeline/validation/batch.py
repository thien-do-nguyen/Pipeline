from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter, delta_table_state
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.control.gold_releases import GoldReleaseStore
from ecommerce_pipeline.ingestion.batch.bronze_operations import bronze_batch_path
from ecommerce_pipeline.pipelines.build_silver import SILVER_PIPELINE_NAME
from ecommerce_pipeline.pipelines.quality import GoldQualityChecker, GoldQualityReport


@dataclass(frozen=True)
class LayerTableReport:
    table_name: str
    bronze_versions: int
    silver_rows: int


@dataclass(frozen=True)
class BatchValidationReport:
    tables: tuple[LayerTableReport, ...]
    gold: GoldQualityReport

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def validate_batch_lakehouse(spark: SparkSession, config: AppConfig) -> BatchValidationReport:
    lakehouse = LakehouseAdapter(spark, config)
    reports: list[LayerTableReport] = []
    current_silver_versions: dict[str, int] = {}
    for table_name, contract in SILVER_TABLES.items():
        bronze_path = bronze_batch_path(config, table_name)
        silver_path = config.lakehouse.table_path("silver", table_name)
        bronze = spark.read.format(config.lakehouse.format).load(bronze_path)
        silver = lakehouse.read_table("silver", table_name)
        bronze_state = delta_table_state(spark, bronze_path, pipeline="postgres_to_bronze")
        silver_state = delta_table_state(
            spark,
            silver_path,
            pipeline=SILVER_PIPELINE_NAME,
        )
        bronze_version = (
            bronze_state.version if bronze_state.progress_version is None else bronze_state.progress_version
        )
        silver_progress = silver_state.progress
        silver_version = None if silver_progress is None else silver_progress.get("last_processed_bronze_version")
        if not isinstance(silver_version, int) or isinstance(silver_version, bool) or silver_version != bronze_version:
            raise ValueError(
                f"Silver progress is not aligned with Bronze for {table_name}: "
                f"bronze_version={bronze_version}, silver_version={silver_version}"
            )
        bronze_progress = bronze_state.progress
        last_event_id = None if bronze_progress is None else bronze_progress.get("last_event_id")
        if not isinstance(last_event_id, int) or isinstance(last_event_id, bool):
            raise ValueError(f"Bronze Delta progress metadata is invalid for {table_name}")
        required_metadata = {
            "_record_hash",
            "_ingested_at",
            "_batch_id",
            "_source_system",
            "_source_schema",
            "_source_table",
            "_ingestion_type",
            "_operation",
            "_event_id",
            "_event_occurred_at",
        }
        missing_metadata = sorted(required_metadata - set(bronze.columns))
        if missing_metadata:
            raise ValueError(f"Missing Bronze metadata in {table_name}: {missing_metadata}")
        bronze_duplicate = bronze.groupBy("_event_id").count().filter(F.col("count") > 1)
        if bronze_duplicate.take(1):
            raise ValueError(f"Duplicate source version in bronze.{table_name}")
        invalid_metadata = F.lit(False)
        for column_name in required_metadata:
            invalid_metadata = invalid_metadata | F.col(column_name).isNull()
        if bronze.filter(invalid_metadata).take(1):
            raise ValueError(f"Null ingestion metadata in bronze.{table_name}")
        duplicate = silver.groupBy(*contract.primary_keys).count().filter(F.col("count") > 1)
        if duplicate.take(1):
            raise ValueError(f"Duplicate key in silver.{table_name}: {contract.primary_keys}")
        null_key = F.lit(False)
        for key in contract.primary_keys:
            null_key = null_key | F.col(key).isNull()
        if silver.filter(null_key).take(1):
            raise ValueError(f"Null primary key in silver.{table_name}")
        if "_bronze_event_id" not in silver.columns:
            raise ValueError(f"Missing incremental lineage in silver.{table_name}: _bronze_event_id")
        if silver.filter(F.col("_bronze_event_id") > F.lit(last_event_id)).take(1):
            raise ValueError(f"Silver contains an event beyond Bronze progress for {table_name}")
        bronze_versions = bronze.count()
        silver_rows = silver.count()
        if silver_rows > bronze_versions:
            raise ValueError(
                f"Silver row count exceeds Bronze source versions for {table_name}: "
                f"silver={silver_rows}, bronze={bronze_versions}"
            )
        reports.append(
            LayerTableReport(
                table_name=table_name,
                bronze_versions=bronze_versions,
                silver_rows=silver_rows,
            )
        )
        if silver_state.progress_version is None:
            raise ValueError(f"Silver Delta progress metadata is missing for {table_name}")
        current_silver_versions[table_name] = silver_state.progress_version
    releases = GoldReleaseStore(spark, config)
    release = releases.latest()
    published_silver_versions = None if release is None else release.silver_versions
    if published_silver_versions != current_silver_versions:
        raise ValueError(
            "Gold progress is not aligned with Silver: "
            f"published_silver_versions={published_silver_versions}, "
            f"current_silver_versions={current_silver_versions}"
        )
    if release is None:
        raise RuntimeError("Gold has no published release")
    gold_report = GoldQualityChecker(lakehouse.with_gold_versions(release.gold_versions)).run()
    return BatchValidationReport(tables=tuple(reports), gold=gold_report)
