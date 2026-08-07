from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.contracts.silver_tables import SilverTableContract

SILVER_SCHEMA_VERSION = 2
SILVER_CHANGE_HISTORY_SUFFIX = "_change_history"
SILVER_SEQUENCE_COLUMNS = (
    "_event_occurred_at",
    "_ingestion_priority",
    "_source_event_sequence",
    "_source_event_subsequence",
)
SILVER_HISTORY_SEQUENCE_COLUMNS = (
    "_event_occurred_at",
    "_ingestion_priority",
    "_source_event_sequence",
    "_source_event_subsequence",
    "_history_event_id",
)

DECIMAL_COLUMNS = {
    "unit_price",
    "compare_at_price",
    "discount_value",
    "max_discount_amount",
    "minimum_order_amount",
    "subtotal_amount",
    "shipping_amount",
    "tax_amount",
    "discount_amount",
    "total_amount",
    "item_subtotal",
    "item_total",
    "amount",
}
INTEGER_COLUMNS = {"quantity", "stock_quantity", "reserved_quantity"}
TIMESTAMP_COLUMNS = {
    "created_at",
    "updated_at",
    "last_login",
    "starts_at",
    "ends_at",
    "paid_at",
    "shipped_at",
    "delivered_at",
}


def clean_text(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    for name in columns:
        if name in df.columns:
            value = F.trim(F.col(name))
            df = df.withColumn(name, F.when(value == "", None).otherwise(value))
    return df


def normalize_lower(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    for name in columns:
        if name in df.columns:
            df = df.withColumn(name, F.lower(F.col(name)))
    return df


def normalize_upper(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    for name in columns:
        if name in df.columns:
            df = df.withColumn(name, F.upper(F.col(name)))
    return df


def normalize_phone(df: DataFrame, column: str = "phone_number") -> DataFrame:
    if column not in df.columns:
        return df
    prefix = F.when(F.col(column).startswith("+"), F.lit("+")).otherwise(F.lit(""))
    digits = F.regexp_replace(F.col(column), "[^0-9]", "")
    return df.withColumn(column, F.when(F.col(column).isNull(), None).otherwise(F.concat(prefix, digits)))


def enforce_types(df: DataFrame) -> DataFrame:
    for name in df.columns:
        if name in DECIMAL_COLUMNS:
            df = df.withColumn(name, F.col(name).cast("decimal(12,2)"))
        elif name == "weight_kg":
            df = df.withColumn(name, F.col(name).cast("decimal(8,3)"))
        elif name in INTEGER_COLUMNS or (name.endswith("_id") and not name.startswith(("public_", "_"))):
            df = df.withColumn(name, F.col(name).cast("int"))
        elif name in TIMESTAMP_COLUMNS:
            df = df.withColumn(name, F.col(name).cast("timestamp"))
        elif name.startswith("is_"):
            df = df.withColumn(name, F.col(name).cast("boolean"))
    return df


def current_state(df: DataFrame, contract: SilverTableContract) -> DataFrame:
    required = {
        *contract.columns,
        "_event_id",
        "_event_occurred_at",
        "_operation",
        "_batch_id",
        "_record_hash",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing Bronze columns for {contract.table_name}: {missing}")
    df = enforce_types(df)
    ingestion_type = F.col("_ingestion_type") if "_ingestion_type" in df.columns else F.lit("batch_change_event")
    ingestion_mode = F.when(ingestion_type == "streaming_cdc", F.lit("cdc")).otherwise(F.lit("batch"))
    ingestion_priority = F.when(ingestion_mode == "cdc", F.lit(2)).otherwise(F.lit(1))
    source_event_sequence = F.when(
        ingestion_mode == "cdc",
        F.coalesce(_optional_column(df, "_source_lsn", "long"), _optional_column(df, "_kafka_offset", "long")),
    ).otherwise(F.col("_event_id").cast("long"))
    source_event_subsequence = F.when(
        ingestion_mode == "cdc",
        F.coalesce(_optional_column(df, "_kafka_offset", "long"), F.lit(-1).cast("long")),
    ).otherwise(F.lit(0).cast("long"))
    df = (
        df.withColumn("_ingestion_mode", ingestion_mode)
        .withColumn("_ingestion_priority", ingestion_priority)
        .withColumn("_source_event_sequence", source_event_sequence)
        .withColumn("_source_event_subsequence", source_event_subsequence)
    )
    window = Window.partitionBy(*contract.primary_keys).orderBy(
        *(F.col(name).desc_nulls_last() for name in SILVER_SEQUENCE_COLUMNS),
        F.col("_event_id").cast("string").desc(),
    )
    columns = [F.col(name) for name in contract.columns]
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(
            *columns,
            F.col("_operation"),
            (F.col("_operation") == F.lit("DELETE")).alias("_is_deleted"),
            F.col("_event_occurred_at"),
            F.col("_event_id").cast("string").alias("_bronze_event_id"),
            F.col("_batch_id").alias("_bronze_batch_id"),
            F.col("_ingestion_mode"),
            F.col("_ingestion_priority"),
            F.col("_source_event_sequence"),
            F.col("_source_event_subsequence"),
            _optional_column(df, "_source_lsn", "long").alias("_source_lsn"),
            _optional_column(df, "_kafka_partition", "int").alias("_kafka_partition"),
            _optional_column(df, "_kafka_offset", "long").alias("_kafka_offset"),
            F.current_timestamp().alias("_silver_updated_at"),
            F.lit(contract.table_name).alias("_source_table"),
            F.lit(SILVER_SCHEMA_VERSION).alias("_silver_schema_version"),
        )
    )


def silver_change_history_table_name(table_name: str) -> str:
    """Return the physical Silver immutable history table for a source entity."""

    return f"{table_name}{SILVER_CHANGE_HISTORY_SUFFIX}"


def change_history(df: DataFrame, contract: SilverTableContract) -> DataFrame:
    """Normalize Bronze events into an append-only Silver Change History table.

    Silver Current intentionally collapses a batch to the newest row per
    business key. SCD Type 2 needs the opposite: every INSERT/UPDATE/DELETE
    event must survive in deterministic source order. This function therefore
    applies the same type normalization and source ordering metadata as
    ``current_state`` but never de-duplicates or deletes history records.
    """

    required = {
        *contract.columns,
        "_event_id",
        "_event_occurred_at",
        "_operation",
        "_batch_id",
        "_record_hash",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing Bronze columns for {contract.table_name}: {missing}")
    df = enforce_types(df)
    ingestion_type = F.col("_ingestion_type") if "_ingestion_type" in df.columns else F.lit("batch_change_event")
    ingestion_mode = F.when(ingestion_type == "streaming_cdc", F.lit("cdc")).otherwise(F.lit("batch"))
    ingestion_priority = F.when(ingestion_mode == "cdc", F.lit(2)).otherwise(F.lit(1))
    source_event_sequence = F.when(
        ingestion_mode == "cdc",
        F.coalesce(_optional_column(df, "_source_lsn", "long"), _optional_column(df, "_kafka_offset", "long")),
    ).otherwise(F.col("_event_id").cast("long"))
    source_event_subsequence = F.when(
        ingestion_mode == "cdc",
        F.coalesce(_optional_column(df, "_kafka_offset", "long"), F.lit(-1).cast("long")),
    ).otherwise(F.lit(0).cast("long"))
    history_event_id = F.sha2(
        F.concat_ws(
            "||",
            F.lit(contract.table_name),
            F.col("_event_id").cast("string"),
            F.col("_batch_id").cast("string"),
            _optional_column(df, "_source_lsn", "long").cast("string"),
            _optional_column(df, "_kafka_partition", "int").cast("string"),
            _optional_column(df, "_kafka_offset", "long").cast("string"),
            F.col("_operation").cast("string"),
        ),
        256,
    )
    return (
        df.withColumn("_ingestion_mode", ingestion_mode)
        .withColumn("_ingestion_priority", ingestion_priority)
        .withColumn("_source_event_sequence", source_event_sequence)
        .withColumn("_source_event_subsequence", source_event_subsequence)
        .select(
            *(F.col(name) for name in contract.columns),
            F.col("_operation"),
            F.col("_event_occurred_at"),
            F.col("_event_id").cast("string").alias("_bronze_event_id"),
            history_event_id.alias("_history_event_id"),
            F.col("_batch_id").alias("_bronze_batch_id"),
            F.col("_record_hash").cast("string").alias("_bronze_record_hash"),
            F.col("_ingestion_mode"),
            F.col("_ingestion_priority"),
            F.col("_source_event_sequence"),
            F.col("_source_event_subsequence"),
            _optional_column(df, "_source_lsn", "long").alias("_source_lsn"),
            _optional_column(df, "_kafka_partition", "int").alias("_kafka_partition"),
            _optional_column(df, "_kafka_offset", "long").alias("_kafka_offset"),
            F.current_timestamp().alias("_silver_history_ingested_at"),
            F.lit(contract.table_name).alias("_source_table"),
            F.lit(SILVER_SCHEMA_VERSION).alias("_silver_schema_version"),
        )
    )


def _optional_column(df: DataFrame, name: str, data_type: str) -> Column:
    return F.col(name).cast(data_type) if name in df.columns else F.lit(None).cast(data_type)
