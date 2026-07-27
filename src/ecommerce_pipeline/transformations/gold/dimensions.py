from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def natural_hash(*columns: Column) -> Column:
    values = [F.coalesce(column.cast("string"), F.lit("")) for column in columns]
    return F.sha2(F.concat_ws("||", *values), 256)


def positive_hash_key(column: Column) -> Column:
    return F.pmod(F.xxhash64(column), F.lit(9_223_372_036_854_775_807)).cast("long")


def second_of_day_key(column: Column) -> Column:
    """Return 1..86400 and reserve key 0 for the unknown time member."""

    return (F.hour(column) * 3600 + F.minute(column) * 60 + F.second(column) + 1).cast("int")


ADDRESS_FIELDS = (
    "address_type",
    "recipient_name",
    "phone_number",
    "street",
    "ward",
    "district",
    "city",
    "state",
    "postal_code",
    "country",
)


def location_hash(*columns: Column) -> Column:
    return natural_hash(*columns)


def location_hash_from_json(column: Column) -> Column:
    return location_hash(*(F.get_json_object(column.cast("string"), f"$.{name}") for name in ADDRESS_FIELDS))


def order_promotion_keys(order_vouchers: DataFrame, vouchers: DataFrame) -> DataFrame:
    grouped = (
        order_vouchers.join(vouchers, "voucher_id", "left")
        .groupBy("order_id")
        .agg(
            F.count("voucher_id").cast("int").alias("voucher_count"),
            F.concat_ws(",", F.sort_array(F.collect_set("voucher_code"))).alias("voucher_codes"),
            F.concat_ws(",", F.sort_array(F.collect_set("voucher_name"))).alias("voucher_names"),
            F.concat_ws(",", F.sort_array(F.collect_set("discount_type"))).alias("discount_types"),
            F.concat_ws(",", F.sort_array(F.collect_set(F.col("scope_json").cast("string")))).alias("promotion_scope"),
            F.min("starts_at").alias("promotion_start_at"),
            F.max("ends_at").alias("promotion_end_at"),
            F.max("minimum_order_amount").alias("minimum_order_amount"),
            F.max(F.col("is_active").cast("int")).cast("boolean").alias("is_active"),
        )
    )
    natural = natural_hash(F.col("voucher_codes"), F.col("discount_types"), F.col("promotion_scope"))
    return grouped.withColumn("natural_promotion_hash", natural).withColumn("promotion_key", positive_hash_key(natural))


def build_dim_date(orders: DataFrame, spark: SparkSession) -> DataFrame:
    dates = (
        orders.select(F.to_date("created_at").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .distinct()
        .select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            "full_date",
            F.dayofweek("full_date").cast("smallint").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.dayofmonth("full_date").cast("smallint").alias("day_of_month"),
            F.dayofyear("full_date").cast("smallint").alias("day_of_year"),
            F.weekofyear("full_date").cast("smallint").alias("week_of_year"),
            F.month("full_date").cast("smallint").alias("month_number"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.quarter("full_date").cast("smallint").alias("quarter_number"),
            F.year("full_date").cast("smallint").alias("year_number"),
            (F.dayofweek("full_date").isin(1, 7)).alias("is_weekend"),
            (F.col("full_date") == F.last_day("full_date")).alias("is_month_end"),
            F.when(F.month("full_date").isin(3, 4, 5), "Spring")
            .when(F.month("full_date").isin(6, 7, 8), "Summer")
            .when(F.month("full_date").isin(9, 10, 11), "Autumn")
            .otherwise("Winter")
            .alias("season_name"),
            F.year("full_date").cast("smallint").alias("fiscal_year"),
            F.quarter("full_date").cast("smallint").alias("fiscal_quarter"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    unknown = spark.sql(
        """
        SELECT 0 date_key, CAST(NULL AS DATE) full_date, CAST(NULL AS SMALLINT) day_of_week,
               'Unknown' day_name, CAST(NULL AS SMALLINT) day_of_month, CAST(NULL AS SMALLINT) day_of_year,
               CAST(NULL AS SMALLINT) week_of_year, CAST(NULL AS SMALLINT) month_number,
               'Unknown' month_name, CAST(NULL AS SMALLINT) quarter_number, CAST(NULL AS SMALLINT) year_number,
               CAST(NULL AS BOOLEAN) is_weekend, CAST(NULL AS BOOLEAN) is_month_end, 'Unknown' season_name,
               CAST(NULL AS SMALLINT) fiscal_year, CAST(NULL AS SMALLINT) fiscal_quarter,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(dates)


def build_dim_time(orders: DataFrame, spark: SparkSession) -> DataFrame:
    times = (
        orders.select("created_at")
        .where(F.col("created_at").isNotNull())
        .select(
            second_of_day_key(F.col("created_at")).alias("time_key"),
            F.date_format("created_at", "HH:mm:ss").alias("full_time"),
            F.hour("created_at").cast("smallint").alias("hour_24"),
            F.minute("created_at").cast("smallint").alias("minute_number"),
            F.second("created_at").cast("smallint").alias("second_number"),
            F.date_format("created_at", "a").alias("am_pm"),
            F.when(F.hour("created_at").between(5, 11), "morning")
            .when(F.hour("created_at").between(12, 17), "afternoon")
            .when(F.hour("created_at").between(18, 21), "evening")
            .otherwise("night")
            .alias("day_part"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["time_key"])
    )
    unknown = spark.sql(
        """
        SELECT 0 time_key, CAST(NULL AS STRING) full_time, CAST(NULL AS SMALLINT) hour_24,
               CAST(NULL AS SMALLINT) minute_number, CAST(NULL AS SMALLINT) second_number,
               'NA' am_pm, 'Unknown' day_part, current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(times)


def build_dim_customer(users: DataFrame, spark: SparkSession) -> DataFrame:
    full_name = F.trim(F.concat_ws(" ", "first_name", "last_name"))
    attrs = natural_hash(
        F.col("username"),
        F.col("email"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("phone_number"),
        F.col("status"),
    )
    rows = users.select(
        positive_hash_key(F.concat_ws("||", "user_id", "updated_at", attrs)).alias("customer_key"),
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
        F.coalesce("updated_at", "created_at").alias("effective_from"),
        F.lit("9999-12-31").cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) customer_key, CAST(NULL AS INT) source_customer_id,
               CAST(NULL AS STRING) public_customer_id, 'unknown' username, CAST(NULL AS STRING) email,
               CAST(NULL AS STRING) first_name, CAST(NULL AS STRING) last_name, 'Unknown Customer' full_name,
               CAST(NULL AS STRING) phone_number, 'unknown' customer_status, CAST(NULL AS STRING) attribute_hash,
               CAST(NULL AS TIMESTAMP) registered_at, CAST(NULL AS TIMESTAMP) last_login_at,
               TIMESTAMP '1970-01-01' effective_from, TIMESTAMP '9999-12-31' effective_to, TRUE is_current,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_location(addresses: DataFrame, orders: DataFrame, spark: SparkSession) -> DataFrame:
    natural = location_hash(*(F.col(name) for name in ADDRESS_FIELDS))
    rows = addresses.select(
        positive_hash_key(natural).alias("location_key"),
        F.col("address_id").alias("source_address_id"),
        F.col("user_id").alias("source_customer_id"),
        natural.alias("natural_location_hash"),
        *ADDRESS_FIELDS,
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    snapshots = []
    for snapshot_column, address_id_column in (
        ("shipping_address_snapshot", "shipping_address_id"),
        ("billing_address_snapshot", "billing_address_id"),
    ):
        snapshot_hash = location_hash_from_json(F.col(snapshot_column))
        snapshots.append(
            orders.select(
                positive_hash_key(snapshot_hash).alias("location_key"),
                F.col(address_id_column).alias("source_address_id"),
                F.col("customer_id").alias("source_customer_id"),
                snapshot_hash.alias("natural_location_hash"),
                *(
                    F.get_json_object(F.col(snapshot_column).cast("string"), f"$.{name}").alias(name)
                    for name in ADDRESS_FIELDS
                ),
                F.current_timestamp().alias("created_at"),
                F.current_timestamp().alias("updated_at"),
            )
        )
    rows = rows.unionByName(snapshots[0]).unionByName(snapshots[1]).dropDuplicates(["location_key"])
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) location_key, CAST(NULL AS INT) source_address_id,
               CAST(NULL AS INT) source_customer_id, 'unknown' natural_location_hash, 'unknown' address_type,
               CAST(NULL AS STRING) recipient_name, CAST(NULL AS STRING) phone_number, CAST(NULL AS STRING) street,
               CAST(NULL AS STRING) ward, CAST(NULL AS STRING) district, 'Unknown' city, CAST(NULL AS STRING) state,
               CAST(NULL AS STRING) postal_code, 'Unknown' country,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_shop(shops: DataFrame, spark: SparkSession) -> DataFrame:
    attrs = natural_hash(
        F.col("public_shop_id"),
        F.col("shop_name"),
        F.col("shop_slug"),
        F.col("status"),
    )
    effective_from = F.coalesce(F.col("updated_at"), F.col("created_at"))
    rows = shops.select(
        positive_hash_key(F.concat_ws("||", "shop_id", effective_from, attrs)).alias("shop_key"),
        F.col("shop_id").alias("source_shop_id"),
        F.col("public_shop_id").alias("public_shop_id"),
        "shop_name",
        "shop_slug",
        F.col("status").alias("shop_status"),
        attrs.alias("attribute_hash"),
        F.col("created_at").alias("shop_created_at"),
        effective_from.alias("effective_from"),
        F.lit("9999-12-31").cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) shop_key, CAST(NULL AS INT) source_shop_id, CAST(NULL AS STRING) public_shop_id,
               'Unknown Shop' shop_name, CAST(NULL AS STRING) shop_slug, 'unknown' shop_status,
               CAST(NULL AS STRING) attribute_hash, CAST(NULL AS TIMESTAMP) shop_created_at,
               TIMESTAMP '1970-01-01' effective_from, TIMESTAMP '9999-12-31' effective_to, TRUE is_current,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_category(categories: DataFrame, spark: SparkSession) -> DataFrame:
    parent = categories.select(
        F.col("category_id").alias("parent_id"),
        F.col("category_name").alias("parent_category_name"),
        F.col("updated_at").alias("parent_updated_at"),
    ).alias("parent")
    current = categories.alias("category")
    joined = current.join(parent, F.col("category.parent_category_id") == F.col("parent.parent_id"), "left")
    attrs = natural_hash(
        F.col("category.parent_category_id"),
        F.col("category.category_name"),
        F.col("category.slug"),
        F.col("parent_category_name"),
        F.col("category.is_active"),
    )
    effective_from = F.greatest(
        F.coalesce(F.col("category.updated_at"), F.col("category.created_at")),
        F.col("parent.parent_updated_at"),
    )
    rows = joined.select(
        positive_hash_key(F.concat_ws("||", F.col("category.category_id"), effective_from, attrs)).alias(
            "category_key"
        ),
        F.col("category.category_id").alias("source_category_id"),
        F.col("category.parent_category_id").alias("source_parent_category_id"),
        F.col("category.category_name").alias("category_name"),
        F.col("category.slug").alias("category_slug"),
        F.col("parent.parent_category_name"),
        F.col("category.is_active"),
        attrs.alias("attribute_hash"),
        F.col("category.created_at").alias("category_created_at"),
        effective_from.alias("effective_from"),
        F.lit("9999-12-31").cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) category_key, CAST(NULL AS INT) source_category_id,
               CAST(NULL AS INT) source_parent_category_id, 'Unknown Category' category_name,
               'unknown' category_slug, CAST(NULL AS STRING) parent_category_name, FALSE is_active,
               CAST(NULL AS STRING) attribute_hash, CAST(NULL AS TIMESTAMP) category_created_at,
               TIMESTAMP '1970-01-01' effective_from, TIMESTAMP '9999-12-31' effective_to, TRUE is_current,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_product(products: DataFrame, variants: DataFrame, spark: SparkSession) -> DataFrame:
    product_cols = products.select(
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
        F.col("updated_at").alias("product_updated_at"),
    )
    variant_cols = variants.select(
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
        F.col("updated_at").alias("variant_updated_at"),
    )
    joined = variant_cols.join(product_cols, "product_id", "left")
    attrs = natural_hash(
        F.col("product_id"),
        F.col("source_shop_id"),
        F.col("source_category_id"),
        F.col("public_product_id"),
        F.col("public_variant_id"),
        F.col("product_sku"),
        F.col("product_slug"),
        F.col("product_name"),
        F.col("brand"),
        F.col("product_status"),
        F.col("is_featured"),
        F.col("variant_sku"),
        F.col("variant_name"),
        F.col("variant_status"),
        F.col("variant_options_json"),
        F.col("is_default_variant"),
        F.col("currency"),
        F.col("weight_kg"),
        F.col("product_attributes_json"),
        F.col("product_images_json"),
        F.col("variant_images_json"),
    )
    effective_from = F.greatest(
        F.coalesce(F.col("product_updated_at"), F.col("product_created_at")),
        F.coalesce(F.col("variant_updated_at"), F.col("variant_created_at")),
    )
    rows = joined.select(
        positive_hash_key(F.concat_ws("||", "product_variant_id", effective_from, attrs)).alias("product_key"),
        F.col("product_id").alias("source_product_id"),
        F.col("product_variant_id").alias("source_product_variant_id"),
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
        attrs.alias("attribute_hash"),
        effective_from.alias("effective_from"),
        F.lit("9999-12-31").cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) product_key, CAST(NULL AS INT) source_product_id,
               CAST(NULL AS INT) source_product_variant_id, CAST(NULL AS INT) source_shop_id,
               CAST(NULL AS INT) source_category_id, CAST(NULL AS STRING) public_product_id,
               CAST(NULL AS STRING) public_variant_id, 'unknown' product_sku, CAST(NULL AS STRING) product_slug,
               'Unknown Product' product_name, CAST(NULL AS STRING) brand, 'unknown' product_status, FALSE is_featured,
               'unknown' variant_sku, 'Unknown Variant' variant_name, 'unknown' variant_status,
               CAST(NULL AS STRING) variant_options_json, FALSE is_default_variant,
               CAST(NULL AS DECIMAL(12,2)) current_unit_price, CAST(NULL AS DECIMAL(12,2)) compare_at_price,
               CAST(NULL AS STRING) currency, CAST(NULL AS INT) stock_quantity, CAST(NULL AS INT) reserved_quantity,
               CAST(NULL AS DECIMAL(8,3)) weight_kg, CAST(NULL AS STRING) product_attributes_json,
               CAST(NULL AS STRING) product_images_json, CAST(NULL AS STRING) variant_images_json,
               CAST(NULL AS TIMESTAMP) product_created_at, CAST(NULL AS TIMESTAMP) variant_created_at,
               CAST(NULL AS STRING) attribute_hash, TIMESTAMP '1970-01-01' effective_from,
               TIMESTAMP '9999-12-31' effective_to, TRUE is_current,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_promotion(order_vouchers: DataFrame, vouchers: DataFrame, spark: SparkSession) -> DataFrame:
    rows = (
        order_promotion_keys(order_vouchers, vouchers)
        .select(
            "promotion_key",
            "natural_promotion_hash",
            F.when(F.col("voucher_count") > 0, "voucher").otherwise("none").alias("promotion_type"),
            "voucher_count",
            "voucher_codes",
            "voucher_names",
            "discount_types",
            "promotion_scope",
            "promotion_start_at",
            "promotion_end_at",
            "minimum_order_amount",
            "is_active",
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
        .dropDuplicates(["promotion_key"])
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) promotion_key, 'none' natural_promotion_hash, 'none' promotion_type,
               0 voucher_count, 'NO_VOUCHER' voucher_codes, CAST(NULL AS STRING) voucher_names,
               CAST(NULL AS STRING) discount_types, 'none' promotion_scope, CAST(NULL AS TIMESTAMP) promotion_start_at,
               CAST(NULL AS TIMESTAMP) promotion_end_at, CAST(NULL AS DECIMAL(12,2)) minimum_order_amount,
               FALSE is_active, current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_payment(payments: DataFrame, spark: SparkSession) -> DataFrame:
    natural = natural_hash(F.col("payment_method"), F.col("payment_status"))
    rows = (
        payments.select("payment_method", "payment_status")
        .distinct()
        .withColumn("natural_payment_hash", natural)
        .select(
            positive_hash_key(F.col("natural_payment_hash")).alias("payment_key"),
            "natural_payment_hash",
            "payment_method",
            F.col("payment_method").alias("payment_method_group"),
            "payment_status",
            (F.col("payment_status") == F.lit("paid")).alias("paid_flag"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) payment_key, 'unknown' natural_payment_hash, 'unknown' payment_method,
               'unknown' payment_method_group, 'unknown' payment_status, FALSE paid_flag,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)


def build_dim_shipping(shipments: DataFrame, spark: SparkSession) -> DataFrame:
    natural = natural_hash(F.col("carrier"), F.col("shipment_status"))
    rows = (
        shipments.select("carrier", "shipment_status")
        .distinct()
        .withColumn("natural_shipping_hash", natural)
        .select(
            positive_hash_key(F.col("natural_shipping_hash")).alias("shipping_key"),
            "natural_shipping_hash",
            "carrier",
            "shipment_status",
            F.col("shipment_status").isin("picked_up", "in_transit", "delivered").alias("shipped_flag"),
            (F.col("shipment_status") == F.lit("delivered")).alias("delivered_flag"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
    )
    unknown = spark.sql(
        """
        SELECT CAST(0 AS BIGINT) shipping_key, 'unknown' natural_shipping_hash, 'unknown' carrier,
               'unknown' shipment_status, FALSE shipped_flag, FALSE delivered_flag,
               current_timestamp() created_at, current_timestamp() updated_at
        """
    )
    return unknown.unionByName(rows)
