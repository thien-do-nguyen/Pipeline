from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.transformations.gold.dimensions import (
    location_hash_from_json,
    natural_hash,
    order_promotion_keys,
    positive_hash_key,
    second_of_day_key,
)


def latest_by_order(df: DataFrame, order_column: str, primary_key: str) -> DataFrame:
    window = Window.partitionBy("order_id").orderBy(
        F.col(order_column).desc_nulls_last(), F.col(primary_key).desc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def _residual_allocation(total: Column, weight: Column, weight_total: Column, order_item_id: Column) -> Column:
    """Allocate money to lines and put any rounding residual on the last line."""

    partition = Window.partitionBy("order_id")
    last_line = Window.partitionBy("order_id").orderBy(order_item_id.desc())
    raw = F.when(weight_total > 0, F.bround(total * weight / weight_total, 2)).otherwise(F.lit(0))
    raw_sum = F.sum(raw).over(partition)
    return F.when(F.row_number().over(last_line) == 1, total - (raw_sum - raw)).otherwise(raw)


def _first_scd2_version(
    dimension: DataFrame,
    source_key: str,
    surrogate_key: str,
    alias: str,
    *extra_columns: str,
) -> DataFrame:
    return (
        dimension.filter(F.col(source_key).isNotNull())
        .select(source_key, surrogate_key, "effective_from", *extra_columns)
        .withColumn(
            "_rn",
            F.row_number().over(Window.partitionBy(source_key).orderBy("effective_from", surrogate_key)),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .alias(alias)
    )


def build_fact_sales(tables: Mapping[str, DataFrame], dimensions: Mapping[str, DataFrame]) -> DataFrame:
    orders = tables["orders"].alias("o")
    items = tables["order_items"].alias("oi")
    payments = latest_by_order(tables["payments"], "updated_at", "payment_id").alias("pay")
    shipments = latest_by_order(tables["shipments"], "updated_at", "shipment_id").alias("ship")
    promotions = (
        order_promotion_keys(tables["order_vouchers"], tables["vouchers"])
        .select("order_id", "promotion_key")
        .alias("promo")
    )
    customer_versions = (
        dimensions["dim_customer"]
        .filter(F.col("source_customer_id").isNotNull())
        .select("source_customer_id", "customer_key", "effective_from", "effective_to")
        .alias("customer")
    )
    first_customer_versions = _first_scd2_version(
        dimensions["dim_customer"],
        "source_customer_id",
        "customer_key",
        "first_customer",
    )
    product_versions = (
        dimensions["dim_product"]
        .filter(F.col("source_product_variant_id").isNotNull())
        .select(
            "source_product_variant_id",
            "source_category_id",
            "product_key",
            "effective_from",
            "effective_to",
        )
        .alias("product")
    )
    first_product_versions = _first_scd2_version(
        dimensions["dim_product"],
        "source_product_variant_id",
        "product_key",
        "first_product",
        "source_category_id",
    )
    shop_versions = (
        dimensions["dim_shop"]
        .filter(F.col("source_shop_id").isNotNull())
        .select("source_shop_id", "shop_key", "effective_from", "effective_to")
        .alias("shop")
    )
    first_shop_versions = _first_scd2_version(
        dimensions["dim_shop"],
        "source_shop_id",
        "shop_key",
        "first_shop",
    )
    category_versions = (
        dimensions["dim_category"]
        .filter(F.col("source_category_id").isNotNull())
        .select("source_category_id", "category_key", "effective_from", "effective_to")
        .alias("category")
    )
    first_category_versions = _first_scd2_version(
        dimensions["dim_category"],
        "source_category_id",
        "category_key",
        "first_category",
    )

    customer_match = (
        (F.col("o.customer_id") == F.col("customer.source_customer_id"))
        & (F.col("o.created_at") >= F.col("customer.effective_from"))
        & (F.col("o.created_at") < F.col("customer.effective_to"))
    )
    product_match = (
        (F.col("oi.product_variant_id") == F.col("product.source_product_variant_id"))
        & (F.col("o.created_at") >= F.col("product.effective_from"))
        & (F.col("o.created_at") < F.col("product.effective_to"))
    )
    shop_match = (
        (F.col("oi.shop_id") == F.col("shop.source_shop_id"))
        & (F.col("o.created_at") >= F.col("shop.effective_from"))
        & (F.col("o.created_at") < F.col("shop.effective_to"))
    )
    joined = (
        items.join(orders, "order_id", "left")
        .join(customer_versions, customer_match, "left")
        .join(first_customer_versions, F.col("o.customer_id") == F.col("first_customer.source_customer_id"), "left")
        .join(product_versions, product_match, "left")
        .join(
            first_product_versions,
            F.col("oi.product_variant_id") == F.col("first_product.source_product_variant_id"),
            "left",
        )
        .join(shop_versions, shop_match, "left")
        .join(first_shop_versions, F.col("oi.shop_id") == F.col("first_shop.source_shop_id"), "left")
        .join(
            category_versions,
            (
                F.coalesce(
                    F.col("product.source_category_id"),
                    F.col("first_product.source_category_id"),
                )
                == F.col("category.source_category_id")
            )
            & (F.col("o.created_at") >= F.col("category.effective_from"))
            & (F.col("o.created_at") < F.col("category.effective_to")),
            "left",
        )
        .join(
            first_category_versions,
            F.coalesce(
                F.col("product.source_category_id"),
                F.col("first_product.source_category_id"),
            )
            == F.col("first_category.source_category_id"),
            "left",
        )
        .join(payments, "order_id", "left")
        .join(shipments, "order_id", "left")
        .join(promotions, "order_id", "left")
    )

    order_window = Window.partitionBy("order_id")
    joined = joined.withColumn("_gross", (F.col("oi.quantity") * F.col("oi.unit_price")).cast("decimal(18,2)"))
    joined = joined.withColumn("_gross_total", F.sum("_gross").over(order_window))
    joined = joined.withColumn("_line_discount_total", F.sum("oi.discount_amount").over(order_window))
    zero_money = F.lit(0).cast("decimal(18,2)")
    joined = joined.withColumn(
        "_order_discount_total",
        F.greatest(F.col("o.discount_amount") - F.col("_line_discount_total"), zero_money),
    )
    joined = joined.withColumn(
        "_order_discount_allocated",
        _residual_allocation(
            F.col("_order_discount_total"), F.col("_gross"), F.col("_gross_total"), F.col("oi.order_item_id")
        ),
    )
    joined = joined.withColumn(
        "_shipping_allocated",
        _residual_allocation(
            F.col("o.shipping_amount"), F.col("_gross"), F.col("_gross_total"), F.col("oi.order_item_id")
        ),
    )
    joined = joined.withColumn(
        "_total_discount",
        F.when(
            F.col("_line_discount_total") <= F.col("o.discount_amount"),
            F.col("oi.discount_amount") + F.col("_order_discount_allocated"),
        ).otherwise(
            _residual_allocation(
                F.col("o.discount_amount"), F.col("_gross"), F.col("_gross_total"), F.col("oi.order_item_id")
            )
        ),
    )
    joined = joined.withColumn(
        "_net_sales",
        F.col("_gross") + F.col("oi.tax_amount") + F.col("_shipping_allocated") - F.col("_total_discount"),
    )

    payment_hash = natural_hash(F.col("pay.payment_method"), F.col("pay.payment_status"))
    shipping_hash = natural_hash(F.col("ship.carrier"), F.col("ship.shipment_status"))
    payment_key = F.when(
        F.col("pay.payment_method").isNull() & F.col("pay.payment_status").isNull(), F.lit(0)
    ).otherwise(positive_hash_key(payment_hash))
    shipping_key = F.when(F.col("ship.carrier").isNull() & F.col("ship.shipment_status").isNull(), F.lit(0)).otherwise(
        positive_hash_key(shipping_hash)
    )
    prehistory_customer_key = F.when(
        F.col("o.created_at") < F.col("first_customer.effective_from"), F.col("first_customer.customer_key")
    )
    prehistory_product_key = F.when(
        F.col("o.created_at") < F.col("first_product.effective_from"), F.col("first_product.product_key")
    )
    prehistory_shop_key = F.when(
        F.col("o.created_at") < F.col("first_shop.effective_from"), F.col("first_shop.shop_key")
    )
    prehistory_category_key = F.when(
        F.col("o.created_at") < F.col("first_category.effective_from"), F.col("first_category.category_key")
    )
    source_category_id = F.coalesce(
        F.col("product.source_category_id"),
        F.col("first_product.source_category_id"),
    )
    ship_to_key = positive_hash_key(location_hash_from_json(F.col("o.shipping_address_snapshot")))
    bill_to_key = positive_hash_key(location_hash_from_json(F.col("o.billing_address_snapshot")))

    return joined.select(
        positive_hash_key(F.concat_ws("||", F.col("oi.order_id"), F.col("oi.order_item_id"))).alias("sales_key"),
        F.coalesce(F.date_format("o.created_at", "yyyyMMdd").cast("int"), F.lit(0)).alias("order_date_key"),
        F.coalesce(second_of_day_key(F.col("o.created_at")), F.lit(0)).alias("order_time_key"),
        F.coalesce(F.col("customer.customer_key"), prehistory_customer_key, F.lit(0)).alias("customer_key"),
        F.coalesce(F.col("product.product_key"), prehistory_product_key, F.lit(0)).alias("product_key"),
        F.coalesce(F.col("shop.shop_key"), prehistory_shop_key, F.lit(0)).alias("shop_key"),
        F.coalesce(F.col("category.category_key"), prehistory_category_key, F.lit(0)).alias("category_key"),
        F.coalesce(F.col("promo.promotion_key"), F.lit(0)).alias("promotion_key"),
        payment_key.alias("payment_key"),
        shipping_key.alias("shipping_key"),
        F.coalesce(ship_to_key, F.lit(0)).alias("ship_to_location_key"),
        F.coalesce(bill_to_key, F.lit(0)).alias("bill_to_location_key"),
        F.col("oi.order_id").alias("source_order_id"),
        F.col("oi.order_item_id").alias("source_order_item_id"),
        F.col("o.order_number"),
        F.col("o.order_status"),
        F.col("o.payment_status"),
        F.col("o.customer_id").alias("source_customer_id"),
        F.col("oi.product_id").alias("source_product_id"),
        F.col("oi.product_variant_id").alias("source_product_variant_id"),
        F.col("oi.shop_id").alias("source_shop_id"),
        source_category_id.alias("source_category_id"),
        F.col("oi.currency"),
        F.col("oi.quantity"),
        F.col("oi.unit_price").cast("decimal(18,2)").alias("unit_price_amount"),
        F.col("_gross").cast("decimal(18,2)").alias("gross_sales_amount"),
        F.col("oi.discount_amount").cast("decimal(18,2)").alias("line_discount_amount"),
        F.col("_order_discount_allocated").cast("decimal(18,2)").alias("order_discount_amount_allocated"),
        F.col("_total_discount").cast("decimal(18,2)").alias("total_discount_amount"),
        F.col("oi.tax_amount").cast("decimal(18,2)").alias("tax_amount"),
        F.col("_shipping_allocated").cast("decimal(18,2)").alias("shipping_amount_allocated"),
        F.col("_net_sales").cast("decimal(18,2)").alias("net_sales_amount"),
        F.col("o.created_at").alias("order_created_at"),
        F.col("oi.created_at").alias("order_item_created_at"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )
