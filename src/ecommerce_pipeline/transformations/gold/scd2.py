from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.transformations.gold.dimensions import natural_hash, positive_hash_key

OPEN_ENDED_EFFECTIVE_TO = "9999-12-31"


def build_dim_customer_from_history(
    history: DataFrame, spark: SparkSession, *, include_unknown: bool = True
) -> DataFrame:
    """Replay immutable app_users history into a production SCD Type 2 dimension.

    Business rules:
    - username/email/name/phone/status are Type 2 attributes.
    - last_login_at is Type 1 and updates the active version without opening a
      new SCD2 row.
    - DELETE closes the active version with ``is_deleted=true`` and does not
      insert a replacement row.
    - All events are ordered by source metadata, not processing time.
    """

    events = _customer_history_events(history)
    rows = _replay_single_entity_scd2(
        events,
        source_key="source_customer_id",
        surrogate_key="customer_key",
        initial_effective_from="registered_at",
        type1_columns=("last_login_at",),
        dimension_columns=(
            "source_customer_id",
            "public_customer_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "phone_number",
            "customer_status",
            "attribute_hash",
            "registered_at",
            "last_login_at",
        ),
    )
    if not include_unknown:
        return rows
    return _unknown_customer(spark).unionByName(rows)


def build_dim_customer_incremental(
    current_dimension: DataFrame,
    new_history: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """Build idempotent customer SCD2 upserts from current Gold + new history only."""

    new_events = _customer_history_events(new_history)
    affected_ids = new_events.select("source_customer_id").where("source_customer_id IS NOT NULL").distinct()
    seed = (
        current_dimension.filter(F.col("is_current") & F.col("source_customer_id").isNotNull())
        .join(F.broadcast(affected_ids), "source_customer_id", "left_semi")
        .select(
            "source_customer_id",
            "public_customer_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "phone_number",
            "customer_status",
            "attribute_hash",
            "registered_at",
            "last_login_at",
            F.col("effective_from").alias("_event_time"),
            F.lit("SEED").alias("_operation"),
            F.concat_ws(":", F.lit("seed"), F.col("source_customer_id"), F.col("effective_from")).alias(
                "_history_event_id"
            ),
            F.lit(0).alias("_ingestion_priority"),
            F.lit(-1).cast("long").alias("_source_event_sequence"),
            F.lit(-1).cast("long").alias("_source_event_subsequence"),
            F.lit(None).cast("long").alias("_source_lsn"),
            F.lit(None).cast("int").alias("_kafka_partition"),
            F.lit(None).cast("long").alias("_kafka_offset"),
        )
    )
    scoped_events = seed.unionByName(new_events)
    return _replay_single_entity_scd2(
        scoped_events,
        source_key="source_customer_id",
        surrogate_key="customer_key",
        initial_effective_from="registered_at",
        type1_columns=("last_login_at",),
        dimension_columns=(
            "source_customer_id",
            "public_customer_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "phone_number",
            "customer_status",
            "attribute_hash",
            "registered_at",
            "last_login_at",
        ),
    )


def _customer_history_events(history: DataFrame) -> DataFrame:
    full_name = F.trim(F.concat_ws(" ", "first_name", "last_name"))
    attrs = natural_hash(
        F.col("username"),
        F.col("email"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("phone_number"),
        F.col("status"),
    )
    return history.select(
        F.col("user_id").alias("source_customer_id"),
        F.col("public_user_id").alias("public_customer_id"),
        "username",
        "email",
        "first_name",
        "last_name",
        full_name.alias("full_name"),
        "phone_number",
        F.col("status").alias("customer_status"),
        attrs.alias("attribute_hash"),
        F.col("created_at").alias("registered_at"),
        F.col("last_login").alias("last_login_at"),
        F.coalesce("_event_occurred_at", "updated_at", "created_at").alias("_event_time"),
        F.upper(F.col("_operation")).alias("_operation"),
        F.col("_history_event_id").cast("string").alias("_history_event_id"),
        F.col("_ingestion_priority").cast("int").alias("_ingestion_priority"),
        F.col("_source_event_sequence").cast("long").alias("_source_event_sequence"),
        F.col("_source_event_subsequence").cast("long").alias("_source_event_subsequence"),
        F.col("_source_lsn").cast("long").alias("_source_lsn"),
        F.col("_kafka_partition").cast("int").alias("_kafka_partition"),
        F.col("_kafka_offset").cast("long").alias("_kafka_offset"),
    )


def _replay_single_entity_scd2(
    events: DataFrame,
    *,
    source_key: str,
    surrogate_key: str,
    initial_effective_from: str,
    type1_columns: Sequence[str],
    dimension_columns: Sequence[str],
) -> DataFrame:
    events = events.filter(F.col(source_key).isNotNull()).dropDuplicates(["_history_event_id"])
    order_columns = _event_order_columns()
    entity_window = Window.partitionBy(source_key).orderBy(*order_columns)
    previous_non_delete_hash = F.last(
        F.when(F.col("_operation") != F.lit("DELETE"), F.col("attribute_hash")),
        True,
    ).over(entity_window.rowsBetween(Window.unboundedPreceding, -1))
    previous_operation = F.lag("_operation").over(entity_window)
    is_non_delete = F.col("_operation") != F.lit("DELETE")
    is_type2_start = is_non_delete & (
        previous_non_delete_hash.isNull()
        | ~F.col("attribute_hash").eqNullSafe(previous_non_delete_hash)
        | (previous_operation == F.lit("DELETE"))
    )
    events = events.withColumn("_is_type2_start", is_type2_start).withColumn(
        "_version_number",
        F.sum(F.col("_is_type2_start").cast("int")).over(
            entity_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )

    version_window = (
        Window.partitionBy(source_key, "_version_number")
        .orderBy(*order_columns)
        .rowsBetween(
            Window.unboundedPreceding,
            Window.unboundedFollowing,
        )
    )
    typed_events = events.filter(is_non_delete & (F.col("_version_number") > F.lit(0)))
    for column in type1_columns:
        typed_events = typed_events.withColumn(column, F.last(column, True).over(version_window))

    boundary_window = Window.partitionBy(source_key).orderBy(*order_columns)
    boundaries = (
        events.filter(F.col("_is_type2_start") | (F.col("_operation") == F.lit("DELETE")))
        .withColumn("_next_effective_to", F.lead("_event_time").over(boundary_window))
        .withColumn("_next_operation", F.lead("_operation").over(boundary_window))
        .select("_history_event_id", "_next_effective_to", "_next_operation")
    )
    starts = typed_events.filter(F.col("_is_type2_start")).join(boundaries, "_history_event_id", "left")
    effective_from = (
        F.when(F.col("_operation") == F.lit("SEED"), F.col("_event_time"))
        .when(F.col("_version_number") == F.lit(1), F.coalesce(F.col(initial_effective_from), F.col("_event_time")))
        .otherwise(F.col("_event_time"))
    )
    dimension = starts.select(
        positive_hash_key(F.concat_ws("||", F.col(source_key), effective_from, F.col("attribute_hash"))).alias(
            surrogate_key
        ),
        *(F.col(column) for column in dimension_columns),
        effective_from.alias("effective_from"),
        F.coalesce(F.col("_next_effective_to"), F.lit(OPEN_ENDED_EFFECTIVE_TO).cast("timestamp")).alias("effective_to"),
        F.col("_next_effective_to").isNull().alias("is_current"),
        F.coalesce(F.col("_next_operation") == F.lit("DELETE"), F.lit(False)).alias("is_deleted"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    return dimension


def _event_order_columns() -> tuple[Column, ...]:
    return (
        F.col("_event_time").asc_nulls_last(),
        F.col("_ingestion_priority").asc_nulls_last(),
        F.col("_source_event_sequence").asc_nulls_last(),
        F.col("_source_event_subsequence").asc_nulls_last(),
        F.col("_source_lsn").asc_nulls_last(),
        F.col("_kafka_partition").asc_nulls_last(),
        F.col("_kafka_offset").asc_nulls_last(),
        F.col("_history_event_id").asc_nulls_last(),
    )


def _unknown_customer(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT CAST(0 AS BIGINT) customer_key, CAST(NULL AS INT) source_customer_id,
               CAST(NULL AS STRING) public_customer_id, 'unknown' username, CAST(NULL AS STRING) email,
               CAST(NULL AS STRING) first_name, CAST(NULL AS STRING) last_name, 'Unknown Customer' full_name,
               CAST(NULL AS STRING) phone_number, 'unknown' customer_status, CAST(NULL AS STRING) attribute_hash,
               CAST(NULL AS TIMESTAMP) registered_at, CAST(NULL AS TIMESTAMP) last_login_at,
               TIMESTAMP '1970-01-01' effective_from, TIMESTAMP '9999-12-31' effective_to, TRUE is_current,
               FALSE is_deleted, current_timestamp() created_at, current_timestamp() updated_at
        """
    )
