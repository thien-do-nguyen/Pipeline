from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_date,
    build_dim_location,
    build_dim_payment,
    build_dim_promotion,
    build_dim_shipping,
    build_dim_time,
)


def test_build_dim_date_and_time_creates_deterministic_keys(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [(1, datetime(2026, 3, 15, 14, 30, 45))],
        "order_id int, created_at timestamp",
    )

    dim_date = build_dim_date(orders, spark)
    date_row = dim_date.filter("date_key <> 0").first()
    assert date_row is not None
    assert date_row["date_key"] > 0
    assert date_row["year_number"] == 2026
    assert date_row["month_number"] == 3
    assert date_row["day_name"] is not None

    dim_time = build_dim_time(orders, spark)
    time_row = dim_time.filter("time_key <> 0").first()
    assert time_row is not None
    assert time_row["time_key"] > 0
    assert time_row["full_time"] is not None
    assert time_row["hour_24"] is not None
    assert time_row["minute_number"] is not None
    assert time_row["second_number"] is not None


def test_build_dim_location_extracts_and_hashes_addresses(spark: SparkSession) -> None:
    addresses = spark.createDataFrame(
        [
            (
                1,
                10,
                "shipping",
                "Alice",
                "0900000001",
                "123 Nguyen Hue",
                "Ben Nghe",
                "District 1",
                "Ho Chi Minh City",
                "HCM",
                "70000",
                "VN",
                True,
                datetime(2026, 1, 1),
                datetime(2026, 1, 1),
            )
        ],
        """address_id int, user_id int, address_type string, recipient_name string,
           phone_number string, street string, ward string, district string, city string,
           state string, postal_code string, country string, is_default boolean,
           created_at timestamp, updated_at timestamp""",
    )
    orders = spark.createDataFrame(
        [],
        """order_id int, customer_id int, shipping_address_id int, billing_address_id int,
           shipping_address_snapshot string, billing_address_snapshot string""",
    )

    dim_location = build_dim_location(addresses, orders, spark)
    row = dim_location.filter("source_address_id = 1").first()
    assert row is not None
    assert row["location_key"] > 0
    assert row["city"] == "Ho Chi Minh City"
    assert row["district"] == "District 1"


def test_build_dim_promotion_and_payment_and_shipping(spark: SparkSession) -> None:
    order_vouchers = spark.createDataFrame(
        [(1, 100)],
        "order_id int, voucher_id int",
    )
    vouchers = spark.createDataFrame(
        [
            (
                100,
                "SUMMER20",
                "Summer Promo",
                "percentage",
                json.dumps(["cart"]),
                datetime(2026, 1, 1),
                datetime(2026, 12, 31),
                Decimal("100.00"),
                True,
            )
        ],
        """voucher_id int, voucher_code string, voucher_name string, discount_type string,
           scope_json string, starts_at timestamp, ends_at timestamp, minimum_order_amount decimal(12,2),
           is_active boolean""",
    )
    payments = spark.createDataFrame(
        [("credit_card", "paid")],
        "payment_method string, payment_status string",
    )
    shipments = spark.createDataFrame(
        [("FastExpress", "delivered")],
        "carrier string, shipment_status string",
    )

    promotions = build_dim_promotion(order_vouchers, vouchers, spark)
    promo_row = promotions.filter("promotion_key <> 0").first()
    assert promo_row is not None
    assert promo_row["promotion_key"] > 0
    assert promo_row["promotion_type"] == "voucher"

    dim_payment = build_dim_payment(payments, spark)
    pay_row = dim_payment.filter("payment_key <> 0").first()
    assert pay_row is not None
    assert pay_row["payment_key"] > 0
    assert pay_row["payment_method"] == "credit_card"

    dim_shipping = build_dim_shipping(shipments, spark)
    ship_row = dim_shipping.filter("shipping_key <> 0").first()
    assert ship_row is not None
    assert ship_row["shipping_key"] > 0
    assert ship_row["carrier"] == "FastExpress"
