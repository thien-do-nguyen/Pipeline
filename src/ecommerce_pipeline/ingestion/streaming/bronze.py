from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from ecommerce_pipeline.adapters.lakehouse import (
    delta_table_properties,
    set_delta_table_property,
    write_delta,
)
from ecommerce_pipeline.config.models import StreamingConfig, TableReference

CHANGE_DATA_FEED_PROPERTY = "delta.enableChangeDataFeed"


def start_bronze_stream(
    df: DataFrame,
    reference: TableReference,
    config: StreamingConfig,
    *,
    available_now: bool = False,
) -> StreamingQuery:
    """Start one fault-tolerant append stream into the raw Bronze Delta table."""

    ensure_bronze_target(df, reference)
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .queryName(config.query_name)
        .option("checkpointLocation", config.checkpoint_location)
    )
    writer = (
        writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=config.trigger_interval)
    )
    if reference.is_catalog:
        if reference.storage_path is not None:
            writer = writer.option("path", reference.storage_path)
        return writer.toTable(reference.value)
    return writer.start(reference.value)


def ensure_bronze_target(df: DataFrame, reference: TableReference) -> None:
    """Create the Delta target once and keep CDF enabled for downstream consumers."""

    spark = df.sparkSession
    exists = spark.catalog.tableExists(reference.value) if reference.is_catalog else _is_delta_path(df, reference)
    if not exists:
        empty = spark.createDataFrame([], df.schema)
        writer = empty.write.format("delta").mode("errorifexists").option(CHANGE_DATA_FEED_PROPERTY, "true")
        try:
            write_delta(writer, reference)
        except Exception:
            # Raw and downstream streams may bootstrap concurrently. Only ignore
            # the create race when the other writer produced a valid Delta table.
            if not (
                spark.catalog.tableExists(reference.value) if reference.is_catalog else _is_delta_path(df, reference)
            ):
                raise
        return

    properties = delta_table_properties(spark, reference)
    if properties.get(CHANGE_DATA_FEED_PROPERTY, "false").lower() != "true":
        set_delta_table_property(spark, reference, CHANGE_DATA_FEED_PROPERTY, "true")


def _is_delta_path(df: DataFrame, reference: TableReference) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(df.sparkSession, reference.value)
