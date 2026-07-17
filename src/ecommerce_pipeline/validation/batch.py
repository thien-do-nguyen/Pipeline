from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.batch.bronze_utils import bronze_batch_path
from ecommerce_pipeline.batch.quality import GoldQualityChecker, GoldQualityReport
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_source_tables import SOURCE_TABLES


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
    for table_name, contract in SOURCE_TABLES.items():
        bronze = spark.read.format(config.lakehouse.format).load(bronze_batch_path(config, table_name))
        silver = lakehouse.read_table("silver", table_name)
        required_metadata = {
            "_record_hash",
            "_ingested_at",
            "_batch_id",
            "_source_system",
            "_source_schema",
            "_source_table",
            "_ingestion_type",
            "_operation",
        }
        missing_metadata = sorted(required_metadata - set(bronze.columns))
        if missing_metadata:
            raise ValueError(f"Missing Bronze metadata in {table_name}: {missing_metadata}")
        bronze_identity = [*contract.primary_keys, contract.incremental_column, "_record_hash"]
        bronze_duplicate = bronze.groupBy(*bronze_identity).count().filter(F.col("count") > 1)
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
    gold_report = GoldQualityChecker(lakehouse).run()
    return BatchValidationReport(tables=tuple(reports), gold=gold_report)
