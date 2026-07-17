from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.bronze_source_tables import BronzeTableContract
from ecommerce_pipeline.control.batch_runs import CompositeWatermark


def bronze_batch_path(config: AppConfig, table_name: str) -> str:
    return "/".join((config.lakehouse.base_path.rstrip("/"), "bronze", "batch", table_name))


def add_bronze_metadata(df: DataFrame, config: AppConfig, table_name: str, batch_id: str) -> DataFrame:
    raw_columns = sorted(df.columns)
    record_json = F.to_json(F.struct(*(F.col(name) for name in raw_columns)), {"ignoreNullFields": "false"})
    return (
        df.withColumn("_record_hash", F.sha2(record_json, 256))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_system", F.lit(config.application.source_system))
        .withColumn("_source_schema", F.lit(config.postgres.source_schema))
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_ingestion_type", F.lit("batch"))
        .withColumn("_operation", F.lit("UPSERT"))
    )


def primary_key(contract: BronzeTableContract) -> str:
    if len(contract.primary_keys) != 1:
        raise ValueError(f"Composite primary keys are not supported yet for {contract.table_name}")
    return contract.primary_keys[0]


def read_batch_upper_bound(reader: PostgresReader, config: AppConfig, contract: BronzeTableContract) -> str | None:
    source = f"{config.postgres.source_schema}.{contract.table_name}"
    query = f"SELECT MAX({contract.incremental_column}) AS batch_upper_bound FROM {source}"
    row = reader.read_query(query).first()
    if row is None or row["batch_upper_bound"] is None:
        return None
    return str(row["batch_upper_bound"])


def source_select_query(
    config: AppConfig,
    contract: BronzeTableContract,
    previous_watermark: CompositeWatermark | None,
    batch_upper_bound: str | None,
) -> str:
    source = f"{config.postgres.source_schema}.{contract.table_name}"
    projection = ", ".join(contract.source_columns)
    if batch_upper_bound is None:
        return f"SELECT {projection} FROM {source}"

    pk = primary_key(contract)
    upper_filter = f"{contract.incremental_column} <= {_timestamp_literal(batch_upper_bound)}"
    if previous_watermark is None:
        where = upper_filter
    else:
        # The lower bound is intentionally inclusive. The append anti-join removes
        # records already persisted, while updates to a lower PK at the exact same
        # timestamp are still visible instead of being skipped.
        lower_filter = f"{contract.incremental_column} >= {_timestamp_literal(previous_watermark.column_value)}"
        where = f"{lower_filter} AND {upper_filter}"
    return f"SELECT {projection} FROM {source} WHERE {where} ORDER BY {contract.incremental_column}, {pk}"


def max_watermark(df: DataFrame, contract: BronzeTableContract) -> CompositeWatermark | None:
    pk = primary_key(contract)
    row = (
        df.orderBy(F.col(contract.incremental_column).desc(), F.col(pk).desc())
        .select(contract.incremental_column, pk)
        .first()
    )
    if row is None or row[contract.incremental_column] is None:
        return None
    return CompositeWatermark(
        column_name=contract.incremental_column,
        column_value=str(row[contract.incremental_column]),
        primary_key=pk,
        primary_key_value=str(row[pk]),
    )


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("Cần cài đặt thư viện delta-spark để ghi/merge Delta.") from exc
    return DeltaTable.isDeltaTable(spark, path)


def drop_already_appended(spark: SparkSession, df: DataFrame, path: str, contract: BronzeTableContract) -> DataFrame:
    pk = primary_key(contract)
    identity = [pk, contract.incremental_column, "_record_hash"]
    existing = spark.read.format("delta").load(path).select(*identity).distinct()
    return df.join(existing, on=identity, how="left_anti")


def write_append_only(df: DataFrame, config: AppConfig, output_path: str, table_exists: bool) -> None:
    mode = "append" if table_exists else "overwrite"
    df.write.format(config.lakehouse.format).mode(mode).save(output_path)


def validate_source_frame(df: DataFrame, contract: BronzeTableContract) -> None:
    required = {*contract.source_columns, *contract.primary_keys, contract.incremental_column}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing source columns for {contract.table_name}: {missing}")
    invalid_condition = F.col(contract.incremental_column).isNull()
    for key in contract.primary_keys:
        invalid_condition = invalid_condition | F.col(key).isNull()
    invalid = df.filter(invalid_condition)
    if invalid.take(1):
        raise ValueError(f"Null primary key or watermark in source table: {contract.table_name}")


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _timestamp_literal(value: str) -> str:
    return f"TIMESTAMP {_sql_literal(value)}"
