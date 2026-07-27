from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import delta_commit_metadata
from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.bronze_tables import BronzeTableContract
from ecommerce_pipeline.contracts.change_events import ChangeOperation, EventCursor

CHANGE_EVENT_TABLE = "change_events"


def bronze_batch_path(config: AppConfig, table_name: str) -> str:
    return "/".join((config.lakehouse.base_path.rstrip("/"), "bronze", "batch", table_name))


def add_bronze_metadata(df: DataFrame, config: AppConfig, table_name: str, batch_id: str) -> DataFrame:
    source_columns = sorted(name for name in df.columns if not name.startswith("_"))
    record_json = F.to_json(F.struct(*(F.col(name) for name in source_columns)), {"ignoreNullFields": "false"})
    return (
        df.withColumn("_record_hash", F.sha2(record_json, 256))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_system", F.lit(config.application.source_system))
        .withColumn("_source_schema", F.lit(config.postgres.source_schema))
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_ingestion_type", F.lit("batch_change_event"))
    )


def read_batch_upper_bound(
    reader: PostgresReader,
    config: AppConfig,
    contract: BronzeTableContract,
    cursor: EventCursor | None,
) -> int | None:
    row = reader.read_first(batch_upper_bound_query(config, contract, cursor))
    if row is None or row["batch_upper_bound"] is None:
        return None
    return int(row["batch_upper_bound"])


def batch_upper_bound_query(
    config: AppConfig,
    contract: BronzeTableContract,
    cursor: EventCursor | None,
) -> str:
    event_log = f"{config.postgres.source_schema}.{CHANGE_EVENT_TABLE}"
    lower_bound = cursor.last_event_id if cursor else 0
    return (
        "SELECT MAX(event_id) AS batch_upper_bound FROM ("
        f"SELECT event_id FROM {event_log} "
        f"WHERE source_table = {_sql_literal(contract.table_name)} AND event_id > {lower_bound} "
        f"ORDER BY event_id LIMIT {config.postgres.max_events_per_batch}"
        ") bounded_events"
    )


def change_event_query(
    config: AppConfig,
    contract: BronzeTableContract,
    cursor: EventCursor | None,
    batch_upper_bound: int | None,
) -> str:
    schema = config.postgres.source_schema
    if batch_upper_bound is None:
        return _empty_change_event_query(schema, contract)

    lower_bound = cursor.last_event_id if cursor else 0
    return (
        f"SELECT record.*, e.event_id AS _event_id, e.operation AS _operation, "
        f"e.occurred_at AS _event_occurred_at "
        f"FROM {schema}.{CHANGE_EVENT_TABLE} e "
        f"CROSS JOIN LATERAL jsonb_populate_record(NULL::{schema}.{contract.table_name}, e.row_data) record "
        f"WHERE e.source_table = {_sql_literal(contract.table_name)} "
        f"AND e.event_id > {lower_bound} AND e.event_id <= {batch_upper_bound} "
    )


def jdbc_partition_count(config: AppConfig, cursor: EventCursor | None, batch_upper_bound: int) -> int:
    lower_bound = cursor.last_event_id if cursor else 0
    event_id_span = max(batch_upper_bound - lower_bound, 1)
    required = math.ceil(event_id_span / config.postgres.target_events_per_partition)
    return min(required, config.postgres.max_jdbc_partitions)


def max_event_cursor(df: DataFrame) -> EventCursor | None:
    row = df.agg(F.max("_event_id").alias("last_event_id")).first()
    if row is None or row["last_event_id"] is None:
        return None
    return EventCursor(last_event_id=int(row["last_event_id"]))


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("Cần cài đặt thư viện delta-spark để ghi/merge Delta.") from exc
    return DeltaTable.isDeltaTable(spark, path)


def write_append_only(
    spark: SparkSession,
    df: DataFrame,
    config: AppConfig,
    output_path: str,
    table_exists: bool,
    *,
    table_name: str,
    batch_id: str,
    cursor: EventCursor,
) -> None:
    mode = "append" if table_exists else "overwrite"
    metadata: dict[str, object] = {
        "pipeline": "postgres_to_bronze",
        "source_table": table_name,
        "batch_id": batch_id,
        "last_event_id": cursor.last_event_id,
    }
    writer = df.write.format(config.lakehouse.format).mode(mode).option("mergeSchema", "true")
    if not table_exists:
        writer = writer.option("delta.enableChangeDataFeed", "true")
    with delta_commit_metadata(spark, metadata):
        writer.save(output_path)


def validate_change_events(df: DataFrame, contract: BronzeTableContract) -> None:
    required = {*contract.primary_keys, "_event_id", "_operation", "_event_occurred_at"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing change-event columns for {contract.table_name}: {missing}")
    invalid_condition = (
        F.col("_event_id").isNull()
        | F.col("_event_occurred_at").isNull()
        | F.col("_operation").isNull()
        | ~F.col("_operation").isin(*(operation.value for operation in ChangeOperation))
    )
    for key in contract.primary_keys:
        invalid_condition = invalid_condition | F.col(key).isNull()
    if df.filter(invalid_condition).take(1):
        raise ValueError(f"Invalid change event for source table: {contract.table_name}")


def _empty_change_event_query(schema: str, contract: BronzeTableContract) -> str:
    return (
        f"SELECT t.*, CAST(NULL AS BIGINT) AS _event_id, CAST(NULL AS VARCHAR) AS _operation, "
        f"CAST(NULL AS TIMESTAMPTZ) AS _event_occurred_at FROM {schema}.{contract.table_name} t WHERE FALSE"
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
