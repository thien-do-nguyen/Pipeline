from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.contracts.silver_source_tables import SilverTableContract

SILVER_SCHEMA_VERSION = 4

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
    required = {*contract.columns, "_batch_id", "_ingested_at", "_record_hash"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing Bronze columns for {contract.table_name}: {missing}")
    df = enforce_types(df)
    window = Window.partitionBy(*contract.primary_keys).orderBy(
        F.col(contract.incremental_column).desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_record_hash").desc_nulls_last(),
    )
    columns = [F.col(name) for name in contract.columns]
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(
            *columns,
            F.coalesce(F.col("_operation"), F.lit("UPSERT")).alias("_operation"),
            F.col("_batch_id").alias("_bronze_batch_id"),
            F.current_timestamp().alias("_silver_updated_at"),
            F.lit(contract.table_name).alias("_source_table"),
            F.lit(SILVER_SCHEMA_VERSION).alias("_silver_schema_version"),
        )
    )
