from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_category,
    build_dim_product,
    build_dim_shop,
    natural_hash,
    positive_hash_key,
)

OPEN_ENDED_EFFECTIVE_TO = "9999-12-31"


def _open_end() -> Column:
    return F.lit(OPEN_ENDED_EFFECTIVE_TO).cast("timestamp")


def build_dim_shop_from_history(
    history: DataFrame,
    spark: SparkSession,
    *,
    include_unknown: bool = True,
) -> DataFrame:
    history = _normalize_history_event_time(history, spark)
    attrs = natural_hash(
        F.col("public_shop_id"),
        F.col("shop_name"),
        F.col("shop_slug"),
        F.col("status"),
    )
    events = history.select(
        F.col("shop_id").alias("source_shop_id"),
        "public_shop_id",
        "shop_name",
        "shop_slug",
        F.col("status").alias("shop_status"),
        attrs.alias("attribute_hash"),
        F.col("created_at").alias("shop_created_at"),
        *_history_order_columns(),
    )
    rows = _replay_single_entity_scd2(
        events,
        source_key="source_shop_id",
        surrogate_key="shop_key",
        initial_effective_from="shop_created_at",
        type1_columns=(),
        dimension_columns=(
            "source_shop_id",
            "public_shop_id",
            "shop_name",
            "shop_slug",
            "shop_status",
            "attribute_hash",
            "shop_created_at",
        ),
    )
    return build_dim_shop(history.limit(0), spark).unionByName(rows) if include_unknown else rows


def build_dim_category_from_history(
    history: DataFrame,
    spark: SparkSession,
    *,
    include_unknown: bool = True,
) -> DataFrame:
    """Replay category and parent timelines so parent renames are also versioned."""

    history = _normalize_history_event_time(history, spark)
    states = _history_states(history, "category_id")
    category = states.select(
        F.col("category_id").alias("source_category_id"),
        F.col("parent_category_id").alias("source_parent_category_id"),
        "category_name",
        F.col("slug").alias("category_slug"),
        "is_active",
        F.col("created_at").alias("category_created_at"),
        F.col("_state_from").alias("_category_from"),
        F.col("_state_to").alias("_category_to"),
        F.col("_state_deleted").alias("_category_deleted"),
        F.col("_history_event_id").alias("_category_event_id"),
        *(_prefixed_order_column(name, "category") for name in _ORDER_FIELD_NAMES),
    ).alias("category")
    parent = states.select(
        F.col("category_id").alias("_parent_id"),
        F.col("category_name").alias("_parent_name"),
        F.col("_state_from").alias("_parent_from"),
        F.col("_state_to").alias("_parent_to"),
        F.col("_state_deleted").alias("_parent_deleted"),
        F.col("_history_event_id").alias("_parent_event_id"),
        *(_prefixed_order_column(name, "parent") for name in _ORDER_FIELD_NAMES),
    ).alias("parent")
    overlaps = category.join(
        parent,
        (F.col("category.source_parent_category_id") == F.col("parent._parent_id"))
        & (F.col("category._category_from") < F.coalesce(F.col("parent._parent_to"), _open_end()))
        & (F.col("parent._parent_from") < F.coalesce(F.col("category._category_to"), _open_end())),
        "left",
    )
    event_time = F.greatest(
        F.col("category._category_from"),
        F.coalesce(F.col("parent._parent_from"), F.col("category._category_from")),
    )
    parent_name = F.when(~F.coalesce(F.col("parent._parent_deleted"), F.lit(False)), F.col("parent._parent_name"))
    attrs = natural_hash(
        F.col("category.source_parent_category_id"),
        F.col("category.category_name"),
        F.col("category.category_slug"),
        parent_name,
        F.col("category.is_active"),
    )
    events = overlaps.select(
        F.col("category.source_category_id"),
        F.col("category.source_parent_category_id"),
        F.col("category.category_name"),
        F.col("category.category_slug"),
        parent_name.alias("parent_category_name"),
        F.col("category.is_active"),
        attrs.alias("attribute_hash"),
        F.col("category.category_created_at"),
        event_time.alias("_event_time"),
        F.when(F.col("category._category_deleted"), F.lit("DELETE")).otherwise(F.lit("UPDATE")).alias("_operation"),
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("category"),
                F.col("category._category_event_id"),
                F.coalesce(F.col("parent._parent_event_id"), F.lit("root")),
                event_time.cast("string"),
            ),
            256,
        ).alias("_history_event_id"),
        *_composite_order_columns("category", "parent"),
    )
    rows = _replay_single_entity_scd2(
        events,
        source_key="source_category_id",
        surrogate_key="category_key",
        initial_effective_from="category_created_at",
        type1_columns=(),
        dimension_columns=(
            "source_category_id",
            "source_parent_category_id",
            "category_name",
            "category_slug",
            "parent_category_name",
            "is_active",
            "attribute_hash",
            "category_created_at",
        ),
    )
    return build_dim_category(history.limit(0), spark).unionByName(rows) if include_unknown else rows


def build_dim_product_from_history(
    product_history: DataFrame,
    variant_history: DataFrame,
    spark: SparkSession,
    *,
    include_unknown: bool = True,
) -> DataFrame:
    """Replay the temporal product/variant join at every source-event boundary."""

    product_history = _normalize_history_event_time(product_history, spark)
    variant_history = _normalize_history_event_time(variant_history, spark)
    products = (
        _history_states(product_history, "product_id")
        .select(
            "product_id",
            F.col("shop_id").alias("source_shop_id"),
            F.col("category_id").alias("source_category_id"),
            "public_product_id",
            "product_sku",
            "product_slug",
            "product_name",
            "brand",
            F.col("status").alias("product_status"),
            "is_featured",
            F.col("attributes_json").alias("product_attributes_json"),
            F.col("images_json").alias("product_images_json"),
            F.col("created_at").alias("product_created_at"),
            F.col("_state_from").alias("_product_from"),
            F.col("_state_to").alias("_product_to"),
            F.col("_state_deleted").alias("_product_deleted"),
            F.col("_history_event_id").alias("_product_event_id"),
            *(_prefixed_order_column(name, "product") for name in _ORDER_FIELD_NAMES),
        )
        .alias("product")
    )
    variants = (
        _history_states(variant_history, "product_variant_id")
        .select(
            "product_variant_id",
            "public_variant_id",
            "product_id",
            "variant_sku",
            "variant_name",
            F.col("status").alias("variant_status"),
            F.col("options_json").alias("variant_options_json"),
            F.col("is_default").alias("is_default_variant"),
            F.col("unit_price").alias("current_unit_price"),
            "compare_at_price",
            "currency",
            "stock_quantity",
            "reserved_quantity",
            "weight_kg",
            F.col("images_json").alias("variant_images_json"),
            F.col("created_at").alias("variant_created_at"),
            F.col("_state_from").alias("_variant_from"),
            F.col("_state_to").alias("_variant_to"),
            F.col("_state_deleted").alias("_variant_deleted"),
            F.col("_history_event_id").alias("_variant_event_id"),
            *(_prefixed_order_column(name, "variant") for name in _ORDER_FIELD_NAMES),
        )
        .alias("variant")
    )
    overlaps = variants.join(
        products,
        (F.col("variant.product_id") == F.col("product.product_id"))
        & (F.col("variant._variant_from") < F.coalesce(F.col("product._product_to"), _open_end()))
        & (F.col("product._product_from") < F.coalesce(F.col("variant._variant_to"), _open_end())),
        "inner",
    )
    event_time = F.greatest(F.col("variant._variant_from"), F.col("product._product_from"))
    deleted = F.col("variant._variant_deleted") | F.col("product._product_deleted")
    attrs = natural_hash(
        F.col("product.product_id"),
        F.col("product.source_shop_id"),
        F.col("product.source_category_id"),
        F.col("product.public_product_id"),
        F.col("variant.public_variant_id"),
        F.col("product.product_sku"),
        F.col("product.product_slug"),
        F.col("product.product_name"),
        F.col("product.brand"),
        F.col("product.product_status"),
        F.col("product.is_featured"),
        F.col("variant.variant_sku"),
        F.col("variant.variant_name"),
        F.col("variant.variant_status"),
        F.col("variant.variant_options_json"),
        F.col("variant.is_default_variant"),
        F.col("variant.currency"),
        F.col("variant.weight_kg"),
        F.col("product.product_attributes_json"),
        F.col("product.product_images_json"),
        F.col("variant.variant_images_json"),
    )
    events = overlaps.select(
        F.col("product.product_id").alias("source_product_id"),
        F.col("variant.product_variant_id").alias("source_product_variant_id"),
        F.col("product.source_shop_id"),
        F.col("product.source_category_id"),
        F.col("product.public_product_id"),
        F.col("variant.public_variant_id"),
        F.col("product.product_sku"),
        F.col("product.product_slug"),
        F.col("product.product_name"),
        F.col("product.brand"),
        F.col("product.product_status"),
        F.col("product.is_featured"),
        F.col("variant.variant_sku"),
        F.col("variant.variant_name"),
        F.col("variant.variant_status"),
        F.col("variant.variant_options_json"),
        F.col("variant.is_default_variant"),
        F.col("variant.current_unit_price"),
        F.col("variant.compare_at_price"),
        F.col("variant.currency"),
        F.col("variant.stock_quantity"),
        F.col("variant.reserved_quantity"),
        F.col("variant.weight_kg"),
        F.col("product.product_attributes_json"),
        F.col("product.product_images_json"),
        F.col("variant.variant_images_json"),
        F.col("product.product_created_at"),
        F.col("variant.variant_created_at"),
        attrs.alias("attribute_hash"),
        event_time.alias("_event_time"),
        F.when(deleted, F.lit("DELETE")).otherwise(F.lit("UPDATE")).alias("_operation"),
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("product"),
                F.col("product._product_event_id"),
                F.col("variant._variant_event_id"),
                event_time.cast("string"),
            ),
            256,
        ).alias("_history_event_id"),
        *_composite_order_columns("product", "variant"),
    )
    rows = _replay_single_entity_scd2(
        events,
        source_key="source_product_variant_id",
        surrogate_key="product_key",
        initial_effective_from="variant_created_at",
        type1_columns=("current_unit_price", "compare_at_price", "stock_quantity", "reserved_quantity"),
        dimension_columns=(
            "source_product_id",
            "source_product_variant_id",
            "source_shop_id",
            "source_category_id",
            "public_product_id",
            "public_variant_id",
            "product_sku",
            "product_slug",
            "product_name",
            "brand",
            "product_status",
            "is_featured",
            "variant_sku",
            "variant_name",
            "variant_status",
            "variant_options_json",
            "is_default_variant",
            "current_unit_price",
            "compare_at_price",
            "currency",
            "stock_quantity",
            "reserved_quantity",
            "weight_kg",
            "product_attributes_json",
            "product_images_json",
            "variant_images_json",
            "product_created_at",
            "variant_created_at",
            "attribute_hash",
        ),
    )
    return (
        build_dim_product(product_history.limit(0), variant_history.limit(0), spark).unionByName(rows)
        if include_unknown
        else rows
    )


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

    events = _customer_history_events(_normalize_history_event_time(history, spark))
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
) -> DataFrame:
    """Build idempotent customer SCD2 upserts from current Gold + new history only."""

    new_events = _customer_history_events(_normalize_history_event_time(new_history, new_history.sparkSession))
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


_ORDER_FIELD_NAMES = (
    "_ingestion_priority",
    "_source_event_sequence",
    "_source_event_subsequence",
    "_source_lsn",
    "_kafka_partition",
    "_kafka_offset",
)


def _history_order_columns() -> tuple[Column, ...]:
    return (
        F.coalesce("_event_occurred_at", "updated_at", "created_at").alias("_event_time"),
        F.upper(F.col("_operation")).alias("_operation"),
        F.col("_history_event_id").cast("string").alias("_history_event_id"),
        *(
            F.col(name).cast(data_type).alias(name)
            for name, data_type in zip(
                _ORDER_FIELD_NAMES,
                ("int", "long", "long", "long", "int", "long"),
                strict=True,
            )
        ),
    )


def _normalize_history_event_time(history: DataFrame, spark: SparkSession) -> DataFrame:
    """Align JDBC TIMESTAMPTZ batch events with local business timestamps."""

    if "_ingestion_mode" not in history.columns:
        return history
    timezone = spark.conf.get("spark.sql.session.timeZone") or "UTC"
    return history.withColumn(
        "_event_occurred_at",
        F.when(
            F.col("_ingestion_mode") == F.lit("batch"),
            F.from_utc_timestamp(F.col("_event_occurred_at"), timezone),
        ).otherwise(F.col("_event_occurred_at")),
    )


def _history_states(history: DataFrame, source_key: str) -> DataFrame:
    """Turn full-row change events into deterministic validity intervals."""

    events = history.withColumn(
        "_state_from",
        F.coalesce("_event_occurred_at", "updated_at", "created_at"),
    ).withColumn("_state_deleted", F.upper(F.col("_operation")) == F.lit("DELETE"))
    order = Window.partitionBy(source_key).orderBy(
        F.col("_state_from").asc_nulls_last(),
        *_event_order_columns()[1:],
    )
    return events.withColumn("_state_to", F.lead("_state_from").over(order))


def _prefixed_order_column(name: str, prefix: str) -> Column:
    return F.col(name).alias(f"_{prefix}_{name.removeprefix('_')}")


def _composite_order_columns(left: str, right: str) -> tuple[Column, ...]:
    data_types = ("int", "long", "long", "long", "int", "long")
    return tuple(
        F.greatest(
            F.col(f"{left}._{left}_{name.removeprefix('_')}").cast(data_type),
            F.col(f"{right}._{right}_{name.removeprefix('_')}").cast(data_type),
        ).alias(name)
        for name, data_type in zip(_ORDER_FIELD_NAMES, data_types, strict=True)
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
        .when(
            F.col("_version_number") == F.lit(1),
            F.least(F.coalesce(F.col(initial_effective_from), F.col("_event_time")), F.col("_event_time")),
        )
        .otherwise(F.col("_event_time"))
    )
    return starts.select(
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
