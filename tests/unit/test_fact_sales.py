from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.transformations.gold.dimensions import second_of_day_key
from ecommerce_pipeline.transformations.gold.fact_sales import build_fact_sales


def _empty(spark: SparkSession, schema: str) -> DataFrame:
    return spark.createDataFrame([], schema)


def test_fact_allocations_reconcile_and_customer_join_is_temporal(spark: SparkSession) -> None:
    order_created = datetime(2026, 1, 15, 10, 30)
    snapshot = json.dumps(
        {
            "address_type": "shipping",
            "recipient_name": "A",
            "phone_number": "0901",
            "street": "1 Main",
            "ward": "W1",
            "district": "D1",
            "city": "HCM",
            "state": None,
            "postal_code": "70000",
            "country": "Vietnam",
        }
    )
    orders = spark.createDataFrame(
        [
            (
                1,
                7,
                "ORD-1",
                "confirmed",
                "paid",
                snapshot,
                snapshot,
                Decimal("400.00"),
                Decimal("30.00"),
                Decimal("32.00"),
                Decimal("50.00"),
                Decimal("412.00"),
                order_created,
            )
        ],
        """order_id int, customer_id int, order_number string, order_status string, payment_status string,
           shipping_address_snapshot string, billing_address_snapshot string, subtotal_amount decimal(18,2),
           shipping_amount decimal(18,2), tax_amount decimal(18,2), discount_amount decimal(18,2),
           total_amount decimal(18,2), created_at timestamp""",
    )
    items = spark.createDataFrame(
        [
            (11, 1, 2, 21, 101, "VND", 1, Decimal("100.00"), Decimal("8.00"), Decimal("10.00"), order_created),
            (12, 1, 2, 22, 102, "VND", 1, Decimal("300.00"), Decimal("24.00"), Decimal("0.00"), order_created),
        ],
        """order_item_id int, order_id int, shop_id int, product_id int, product_variant_id int,
           currency string, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2),
           discount_amount decimal(18,2), created_at timestamp""",
    )
    products = spark.createDataFrame([(21, 3), (22, 3)], "product_id int, category_id int")
    payments = spark.createDataFrame(
        [(31, 1, "visa", "paid", datetime(2026, 1, 15, 11))],
        "payment_id int, order_id int, payment_method string, payment_status string, updated_at timestamp",
    )
    shipments = spark.createDataFrame(
        [(41, 1, "GHN", "in_transit", datetime(2026, 1, 16))],
        "shipment_id int, order_id int, carrier string, shipment_status string, updated_at timestamp",
    )
    customers = spark.createDataFrame(
        [
            (7, 701, datetime(2025, 1, 1), datetime(2026, 2, 1)),
            (7, 702, datetime(2026, 2, 1), datetime(9999, 12, 31)),
        ],
        "source_customer_id int, customer_key long, effective_from timestamp, effective_to timestamp",
    )
    tables = {
        "orders": orders,
        "order_items": items,
        "products": products,
        "payments": payments,
        "shipments": shipments,
        "order_vouchers": _empty(
            spark, "order_voucher_id int, order_id int, voucher_id int, discount_amount decimal(18,2)"
        ),
        "vouchers": _empty(
            spark,
            """voucher_id int, voucher_code string, voucher_name string, discount_type string,
               scope_json string, starts_at timestamp, ends_at timestamp, minimum_order_amount decimal(18,2),
               is_active boolean""",
        ),
    }

    fact = build_fact_sales(tables, customers)
    totals = fact.agg({"gross_sales_amount": "sum", "total_discount_amount": "sum", "net_sales_amount": "sum"}).first()
    rows = fact.orderBy("source_order_item_id").collect()

    assert totals is not None
    assert totals["sum(gross_sales_amount)"] == Decimal("400.00")
    assert totals["sum(total_discount_amount)"] == Decimal("50.00")
    assert totals["sum(net_sales_amount)"] == Decimal("412.00")
    assert {row["customer_key"] for row in rows} == {701}
    assert [row["order_discount_amount_allocated"] for row in rows] == [Decimal("10.00"), Decimal("30.00")]
    assert [row["shipping_amount_allocated"] for row in rows] == [Decimal("7.50"), Decimal("22.50")]


def test_fact_uses_first_customer_version_for_orders_before_scd2_history(spark: SparkSession) -> None:
    order_created = datetime(2026, 1, 15, 10, 30)
    snapshot = json.dumps(
        {
            "address_type": "shipping",
            "recipient_name": "A",
            "phone_number": "0901",
            "street": "1 Main",
            "ward": "W1",
            "district": "D1",
            "city": "HCM",
            "state": None,
            "postal_code": "70000",
            "country": "Vietnam",
        }
    )
    orders = spark.createDataFrame(
        [
            (
                1,
                7,
                "ORD-1",
                "confirmed",
                "paid",
                snapshot,
                snapshot,
                Decimal("100.00"),
                Decimal("0.00"),
                Decimal("8.00"),
                Decimal("0.00"),
                Decimal("108.00"),
                order_created,
            )
        ],
        """order_id int, customer_id int, order_number string, order_status string, payment_status string,
           shipping_address_snapshot string, billing_address_snapshot string, subtotal_amount decimal(18,2),
           shipping_amount decimal(18,2), tax_amount decimal(18,2), discount_amount decimal(18,2),
           total_amount decimal(18,2), created_at timestamp""",
    )
    items = spark.createDataFrame(
        [(11, 1, 2, 21, 101, "VND", 1, Decimal("100.00"), Decimal("8.00"), Decimal("0.00"), order_created)],
        """order_item_id int, order_id int, shop_id int, product_id int, product_variant_id int,
           currency string, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2),
           discount_amount decimal(18,2), created_at timestamp""",
    )
    customers = spark.createDataFrame(
        [(7, 900, datetime(2026, 2, 1), datetime(9999, 12, 31))],
        "source_customer_id int, customer_key long, effective_from timestamp, effective_to timestamp",
    )
    tables = {
        "orders": orders,
        "order_items": items,
        "products": spark.createDataFrame([(21, 3)], "product_id int, category_id int"),
        "payments": _empty(
            spark, "payment_id int, order_id int, payment_method string, payment_status string, updated_at timestamp"
        ),
        "shipments": _empty(
            spark, "shipment_id int, order_id int, carrier string, shipment_status string, updated_at timestamp"
        ),
        "order_vouchers": _empty(
            spark, "order_voucher_id int, order_id int, voucher_id int, discount_amount decimal(18,2)"
        ),
        "vouchers": _empty(
            spark,
            """voucher_id int, voucher_code string, voucher_name string, discount_type string,
               scope_json string, starts_at timestamp, ends_at timestamp, minimum_order_amount decimal(18,2),
               is_active boolean""",
        ),
    }

    fact = build_fact_sales(tables, customers)

    assert fact.select("customer_key").first()["customer_key"] == 900


def test_fact_caps_total_discount_to_order_header_when_line_discounts_are_informational(
    spark: SparkSession,
) -> None:
    order_created = datetime(2026, 1, 15, 10, 30)
    snapshot = json.dumps(
        {
            "address_type": "shipping",
            "recipient_name": "A",
            "phone_number": "0901",
            "street": "1 Main",
            "ward": "W1",
            "district": "D1",
            "city": "HCM",
            "state": None,
            "postal_code": "70000",
            "country": "Vietnam",
        }
    )
    orders = spark.createDataFrame(
        [
            (
                1,
                7,
                "ORD-1",
                "confirmed",
                "paid",
                snapshot,
                snapshot,
                Decimal("100.00"),
                Decimal("0.00"),
                Decimal("8.00"),
                Decimal("0.00"),
                Decimal("108.00"),
                order_created,
            )
        ],
        """order_id int, customer_id int, order_number string, order_status string, payment_status string,
           shipping_address_snapshot string, billing_address_snapshot string, subtotal_amount decimal(18,2),
           shipping_amount decimal(18,2), tax_amount decimal(18,2), discount_amount decimal(18,2),
           total_amount decimal(18,2), created_at timestamp""",
    )
    items = spark.createDataFrame(
        [(11, 1, 2, 21, 101, "VND", 1, Decimal("100.00"), Decimal("8.00"), Decimal("5.00"), order_created)],
        """order_item_id int, order_id int, shop_id int, product_id int, product_variant_id int,
           currency string, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2),
           discount_amount decimal(18,2), created_at timestamp""",
    )
    tables = {
        "orders": orders,
        "order_items": items,
        "products": spark.createDataFrame([(21, 3)], "product_id int, category_id int"),
        "payments": _empty(
            spark, "payment_id int, order_id int, payment_method string, payment_status string, updated_at timestamp"
        ),
        "shipments": _empty(
            spark, "shipment_id int, order_id int, carrier string, shipment_status string, updated_at timestamp"
        ),
        "order_vouchers": _empty(
            spark, "order_voucher_id int, order_id int, voucher_id int, discount_amount decimal(18,2)"
        ),
        "vouchers": _empty(
            spark,
            """voucher_id int, voucher_code string, voucher_name string, discount_type string,
               scope_json string, starts_at timestamp, ends_at timestamp, minimum_order_amount decimal(18,2),
               is_active boolean""",
        ),
    }
    customers = spark.createDataFrame(
        [(7, 900, datetime(2026, 1, 1), datetime(9999, 12, 31))],
        "source_customer_id int, customer_key long, effective_from timestamp, effective_to timestamp",
    )

    row = (
        build_fact_sales(tables, customers)
        .select("line_discount_amount", "total_discount_amount", "net_sales_amount")
        .first()
    )

    assert row["line_discount_amount"] == Decimal("5.00")
    assert row["total_discount_amount"] == Decimal("0.00")
    assert row["net_sales_amount"] == Decimal("108.00")


def test_time_key_reserves_zero_for_unknown(spark: SparkSession) -> None:
    times = spark.sql(
        """
        SELECT TIMESTAMP '2026-01-01 00:00:00' AS event_at
        UNION ALL
        SELECT TIMESTAMP '2026-01-01 23:59:59' AS event_at
        """
    )

    keys = [row["time_key"] for row in times.select(second_of_day_key(F.col("event_at")).alias("time_key")).collect()]

    assert keys == [1, 86400]
