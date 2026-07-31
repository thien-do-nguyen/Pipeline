from __future__ import annotations

import math
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import delta_commit_metadata, write_delta
from ecommerce_pipeline.adapters.postgres import PostgresReader
from ecommerce_pipeline.config.models import AppConfig, TableReference
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_SCHEMA_VERSION, BronzeTableContract
from ecommerce_pipeline.contracts.change_events import ChangeOperation, EventCursor

CHANGE_EVENT_TABLE = "change_events"


@dataclass(frozen=True)
class ChangeEventStats:
    record_count: int
    last_event_id: int | None
    operation_counts: dict[str, int]


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


def read_batch_upper_bounds(
    reader: PostgresReader,
    config: AppConfig,
    cursors: dict[str, EventCursor | None],
) -> dict[str, int | None]:
    """Capture all table high-water marks in one PostgreSQL control query."""

    if not cursors:
        return {}
    rows = reader.read_all(batch_upper_bounds_query(config, cursors))
    bounds = {
        str(row["source_table"]): (None if row["batch_upper_bound"] is None else int(row["batch_upper_bound"]))
        for row in rows
    }
    missing = sorted(set(cursors) - set(bounds))
    if missing:
        raise RuntimeError(f"PostgreSQL did not return batch upper bounds for: {missing}")
    return bounds


def batch_upper_bounds_query(
    config: AppConfig,
    cursors: dict[str, EventCursor | None],
) -> str:
    if not cursors:
        raise ValueError("At least one source cursor is required")
    values = ", ".join(
        f"({_sql_literal(table_name)}, {cursor.last_event_id if cursor else 0})"
        for table_name, cursor in cursors.items()
    )
    event_log = f"{config.postgres.source_schema}.{CHANGE_EVENT_TABLE}"
    query = (
        "SELECT cursors.source_table, ("
        "SELECT MAX(event_id) FROM ("
        f"SELECT events.event_id FROM {event_log} events "
        "WHERE events.source_table = cursors.source_table "
        "AND events.event_id > cursors.last_event_id "
        f"ORDER BY events.event_id LIMIT {config.postgres.max_events_per_batch}"
        ") bounded_events"
        ") AS batch_upper_bound "
        f"FROM (VALUES {values}) AS cursors(source_table, last_event_id)"
    )
    return query


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


def write_append_only(
    spark: SparkSession,
    df: DataFrame,
    config: AppConfig,
    reference: TableReference,
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
        "bronze_schema_version": BRONZE_SCHEMA_VERSION,
    }
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if not table_exists:
        writer = writer.option("delta.enableChangeDataFeed", "true")
    with delta_commit_metadata(spark, metadata):
        write_delta(writer, reference)


def collect_change_event_stats(df: DataFrame, contract: BronzeTableContract) -> ChangeEventStats:
    """Validate and measure a cached JDBC batch with one Spark action."""

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
    row = df.agg(
        F.count(F.lit(1)).alias("record_count"),
        F.max("_event_id").alias("last_event_id"),
        F.sum(F.when(invalid_condition, F.lit(1)).otherwise(F.lit(0))).alias("invalid_count"),
        *(
            F.sum(F.when(F.col("_operation") == operation.value, F.lit(1)).otherwise(F.lit(0))).alias(
                f"operation_{operation.value.lower()}"
            )
            for operation in ChangeOperation
        ),
    ).first()
    if row is None:
        raise RuntimeError(f"Change-event metrics are unavailable for: {contract.table_name}")
    if int(row["invalid_count"] or 0):
        raise ValueError(f"Invalid change event for source table: {contract.table_name}")
    return ChangeEventStats(
        record_count=int(row["record_count"]),
        last_event_id=None if row["last_event_id"] is None else int(row["last_event_id"]),
        operation_counts={
            operation.value: int(row[f"operation_{operation.value.lower()}"] or 0) for operation in ChangeOperation
        },
    )


def _empty_change_event_query(schema: str, contract: BronzeTableContract) -> str:
    return (
        f"SELECT t.*, CAST(NULL AS BIGINT) AS _event_id, CAST(NULL AS VARCHAR) AS _operation, "
        f"CAST(NULL AS TIMESTAMPTZ) AS _event_occurred_at FROM {schema}.{contract.table_name} t WHERE FALSE"
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
