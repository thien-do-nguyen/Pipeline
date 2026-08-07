from __future__ import annotations

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DecimalType, TimestampType

from ecommerce_pipeline.config.models import TableReference
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_SCHEMA_VERSION
from ecommerce_pipeline.contracts.cdc_events import DebeziumOperation
from ecommerce_pipeline.contracts.cdc_tables import TYPED_CDC_TABLES, TypedCdcTableContract

_PAYLOAD_MAP_SCHEMA = "MAP<STRING,STRING>"
_REQUIRED_RAW_COLUMNS = {
    "_transport_event_id",
    "topic",
    "partition",
    "offset",
    "value_json",
    "source_schema",
    "source_table",
    "operation",
    "source_lsn",
    "source_tx_id",
    "transaction_id",
    "transaction_order",
    "collection_order",
    "event_timestamp",
    "is_valid",
    "parse_error",
    "ingested_at",
}


def read_raw_bronze_stream(
    spark: SparkSession,
    reference: TableReference,
    *,
    max_files_per_trigger: int | None = None,
    max_bytes_per_trigger: str | None = None,
) -> DataFrame:
    reader = spark.readStream.format("delta")
    if max_files_per_trigger is not None:
        reader = reader.option("maxFilesPerTrigger", max_files_per_trigger)
    if max_bytes_per_trigger is not None:
        reader = reader.option("maxBytesPerTrigger", max_bytes_per_trigger)
    return reader.table(reference.value) if reference.is_catalog else reader.load(reference.value)


def prepare_raw_cdc_events(df: DataFrame) -> DataFrame:
    """Select and validate the Debezium row image consumed by shared Silver."""

    missing = sorted(_REQUIRED_RAW_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Raw CDC Bronze is missing required columns: {missing}")

    before_json = F.get_json_object("value_json", "$.before")
    after_json = F.get_json_object("value_json", "$.after")
    payload_json = F.when(F.col("operation") == DebeziumOperation.DELETE.value, before_json).otherwise(after_json)
    return (
        df.withColumn("_payload_json", payload_json)
        .withColumn(
            "_payload_map",
            F.from_json(payload_json, _PAYLOAD_MAP_SCHEMA, {"primitivesAsString": "true"}),
        )
        .withColumn("_decode_error", _decode_error())
    )


def decode_table_events(
    df: DataFrame,
    contract: TypedCdcTableContract,
    *,
    batch_id: int,
    query_name: str,
    source_system: str,
    timezone: str,
    checkpoint_version: str = "v1",
) -> DataFrame:
    """Convert raw Debezium rows into the common event contract expected by Silver."""

    business_columns = [
        _typed_payload_value(field.name, field.dataType, timezone).alias(field.name)
        for field in contract.payload_schema
    ]
    operation = (
        F.when(
            F.col("operation").isin(DebeziumOperation.SNAPSHOT.value, DebeziumOperation.CREATE.value),
            F.lit("INSERT"),
        )
        .when(F.col("operation") == DebeziumOperation.UPDATE.value, F.lit("UPDATE"))
        .when(F.col("operation") == DebeziumOperation.DELETE.value, F.lit("DELETE"))
    )
    typed = df.select(
        *business_columns,
        F.col("_transport_event_id").alias("_event_id"),
        operation.alias("_operation"),
        F.col("event_timestamp").alias("_event_occurred_at"),
        F.col("source_lsn").alias("_source_lsn"),
        F.col("source_tx_id").alias("_source_tx_id"),
        F.col("transaction_id").alias("_transaction_id"),
        F.col("transaction_order").alias("_transaction_order"),
        F.col("collection_order").alias("_collection_order"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("ingested_at").alias("_ingested_at"),
        F.lit(f"{query_name}:{checkpoint_version}:{batch_id}").alias("_batch_id"),
        F.lit(source_system).alias("_source_system"),
        F.col("source_schema").alias("_source_schema"),
        F.col("source_table").alias("_source_table"),
        F.lit("streaming_cdc").alias("_ingestion_type"),
        F.lit(BRONZE_SCHEMA_VERSION).alias("_bronze_schema_version"),
    )
    record_json = F.to_json(
        F.struct(*(F.col(field.name) for field in contract.payload_schema)),
        {"ignoreNullFields": "false"},
    )
    return typed.withColumn("_record_hash", F.sha2(record_json, 256))


def table_statistics(df: DataFrame) -> dict[str, Row]:
    """Return progress for valid rows; invalid rows are handled by quarantine."""

    rows = (
        df.where(F.col("_decode_error").isNull())
        .groupBy("source_table")
        .agg(
            F.count(F.lit(1)).alias("record_count"),
            F.min("source_lsn").alias("min_source_lsn"),
            F.max("source_lsn").alias("max_source_lsn"),
        )
        .collect()
    )
    return {str(row["source_table"]): row for row in rows}


def _decode_error() -> Column:
    errors = [
        F.when(
            ~F.coalesce(F.col("is_valid"), F.lit(False)),
            F.concat(F.lit("raw:"), F.coalesce(F.col("parse_error"), F.lit("invalid_event"))),
        ),
        F.when(F.col("_transport_event_id").isNull(), F.lit("missing_transport_event_id")),
        F.when(~F.col("source_table").isin(*TYPED_CDC_TABLES), F.lit("unknown_source_table")),
        F.when(F.col("_payload_map").isNull(), F.lit("missing_or_invalid_payload")),
    ]
    for table_name, contract in TYPED_CDC_TABLES.items():
        scope = F.col("source_table") == table_name
        missing_key = F.lit(False)
        for key in contract.primary_keys:
            field = contract.payload_schema[key]
            missing_key = missing_key | _typed_payload_value(key, field.dataType, "UTC").isNull()
        allowed = F.array(*(F.lit(column) for column in contract.columns))
        unexpected = F.array_sort(F.array_except(F.map_keys(F.col("_payload_map")), allowed))
        errors.extend(
            (
                F.when(scope & missing_key, F.lit("missing_or_invalid_primary_key")),
                F.when(
                    scope & (F.size(unexpected) > 0),
                    F.concat(F.lit("unexpected_payload_fields:"), F.array_join(unexpected, ",")),
                ),
            )
        )
        for field in contract.payload_schema:
            raw = F.element_at(F.col("_payload_map"), F.lit(field.name))
            cast_failed = raw.isNotNull() & _typed_payload_value(field.name, field.dataType, "UTC").isNull()
            errors.append(F.when(scope & cast_failed, F.lit(f"invalid_field_type:{field.name}")))
    return F.coalesce(*errors)


def _typed_payload_value(column: str, data_type: DataType, timezone: str) -> Column:
    raw = F.element_at(F.col("_payload_map"), F.lit(column))
    if isinstance(data_type, TimestampType):
        numeric = raw.rlike(r"^-?[0-9]+$")
        return F.when(
            numeric,
            F.to_utc_timestamp(F.timestamp_micros(raw.cast("long")), timezone),
        ).otherwise(F.to_timestamp(raw))
    if isinstance(data_type, DecimalType):
        return raw.cast(data_type)
    return raw.cast(data_type)
