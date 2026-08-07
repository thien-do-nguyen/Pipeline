from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.contracts.cdc_events import CDC_SCHEMA_VERSION, VALID_DEBEZIUM_OPERATIONS

_DEBEZIUM_ENVELOPE = StructType(
    [
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField(
            "source",
            StructType(
                [
                    StructField("schema", StringType()),
                    StructField("table", StringType()),
                    StructField("lsn", LongType()),
                    StructField("txId", LongType()),
                    StructField("ts_ms", LongType()),
                    StructField("snapshot", StringType()),
                ]
            ),
        ),
        StructField(
            "transaction",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("total_order", LongType()),
                    StructField("data_collection_order", LongType()),
                ]
            ),
        ),
    ]
)

_KAFKA_INPUT_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
        StructField(
            "headers",
            ArrayType(
                StructType(
                    [
                        StructField("key", StringType()),
                        StructField("value", BinaryType()),
                    ]
                )
            ),
        ),
    ]
)


def normalize_debezium_events(df: DataFrame) -> DataFrame:
    """Normalize transport metadata while retaining the complete Debezium JSON envelope."""

    decoded = (
        df.withColumn("key_json", F.col("key").cast("string"))
        .withColumn("value_json", F.col("value").cast("string"))
        .withColumn("_envelope", F.from_json("value_json", _DEBEZIUM_ENVELOPE))
    )
    valid_operation = F.col("_envelope.op").isin(*VALID_DEBEZIUM_OPERATIONS)
    has_source = F.col("_envelope.source.schema").isNotNull() & F.col("_envelope.source.table").isNotNull()
    is_valid = F.col("value_json").isNotNull() & F.col("_envelope").isNotNull() & valid_operation & has_source
    parse_error = (
        F.when(F.col("value_json").isNull(), F.lit("tombstone_or_null_value"))
        .when(F.col("_envelope").isNull(), F.lit("invalid_json"))
        .when(~valid_operation, F.lit("unsupported_operation"))
        .when(~has_source, F.lit("missing_source_metadata"))
    )

    return decoded.select(
        F.concat_ws(":", "topic", F.col("partition").cast("string"), F.col("offset").cast("string")).alias(
            "_transport_event_id"
        ),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("broker_timestamp"),
        "headers",
        "key_json",
        "value_json",
        F.col("_envelope.source.schema").alias("source_schema"),
        F.col("_envelope.source.table").alias("source_table"),
        F.col("_envelope.op").alias("operation"),
        F.col("_envelope.source.lsn").alias("source_lsn"),
        F.col("_envelope.source.txId").alias("source_tx_id"),
        F.col("_envelope.source.snapshot").alias("source_snapshot"),
        F.timestamp_millis(F.col("_envelope.source.ts_ms")).alias("source_timestamp"),
        F.timestamp_millis(F.col("_envelope.ts_ms")).alias("event_timestamp"),
        F.col("_envelope.transaction.id").alias("transaction_id"),
        F.col("_envelope.transaction.total_order").alias("transaction_order"),
        F.col("_envelope.transaction.data_collection_order").alias("collection_order"),
        is_valid.alias("is_valid"),
        parse_error.alias("parse_error"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(CDC_SCHEMA_VERSION).alias("schema_version"),
    )


def empty_normalized_debezium_events(spark: SparkSession) -> DataFrame:
    """Build an empty Raw CDC DataFrame from the same normalization plan as Kafka."""

    return normalize_debezium_events(spark.createDataFrame([], _KAFKA_INPUT_SCHEMA))
