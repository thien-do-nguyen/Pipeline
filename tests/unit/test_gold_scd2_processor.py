from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ecommerce_pipeline.transformations.gold.scd2 import (
    build_dim_category_from_history,
    build_dim_customer_from_history,
    build_dim_product_from_history,
    build_dim_shop_from_history,
)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _history(spark: SparkSession, rows: list[dict[str, object]]) -> DataFrame:
    defaults: dict[str, object] = {
        "user_id": 1,
        "public_user_id": "u-1",
        "username": "alice",
        "email": "alice@example.com",
        "first_name": "Alice",
        "last_name": "Nguyen",
        "phone_number": "0900000001",
        "status": "active",
        "created_at": _ts("2026-01-01T10:00:00"),
        "updated_at": None,
        "last_login": None,
        "_operation": "INSERT",
        "_event_occurred_at": _ts("2026-01-01T10:00:00"),
        "_history_event_id": "e1",
        "_ingestion_priority": 1,
        "_source_event_sequence": 1,
        "_source_event_subsequence": 0,
        "_source_lsn": None,
        "_kafka_partition": None,
        "_kafka_offset": None,
    }
    materialized = [Row(**{**defaults, **row}) for row in rows]
    schema = T.StructType(
        [
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField("public_user_id", T.StringType(), True),
            T.StructField("username", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("phone_number", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
            T.StructField("last_login", T.TimestampType(), True),
            T.StructField("_operation", T.StringType(), True),
            T.StructField("_event_occurred_at", T.TimestampType(), True),
            T.StructField("_history_event_id", T.StringType(), True),
            T.StructField("_ingestion_priority", T.IntegerType(), True),
            T.StructField("_source_event_sequence", T.LongType(), True),
            T.StructField("_source_event_subsequence", T.LongType(), True),
            T.StructField("_source_lsn", T.LongType(), True),
            T.StructField("_kafka_partition", T.IntegerType(), True),
            T.StructField("_kafka_offset", T.LongType(), True),
        ]
    )
    return spark.createDataFrame(materialized, schema)


def _business_rows(df: DataFrame) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in (
            df.where("source_customer_id IS NOT NULL")
            .select(
                "source_customer_id",
                "username",
                "email",
                "first_name",
                "last_name",
                "phone_number",
                "last_login_at",
                "effective_from",
                "effective_to",
                "is_current",
                "is_deleted",
            )
            .orderBy("source_customer_id", "effective_from")
            .collect()
        )
    ]


_HISTORY_META_SCHEMA = """
    _operation string, _event_occurred_at timestamp, _history_event_id string,
    _ingestion_priority int, _source_event_sequence long, _source_event_subsequence long,
    _source_lsn long, _kafka_partition int, _kafka_offset long
"""


def test_shop_replay_keeps_intermediate_versions(spark: SparkSession) -> None:
    history = spark.createDataFrame(
        [
            (
                1,
                "s-1",
                "A",
                "a",
                "active",
                _ts("2026-01-01T00:00:00"),
                None,
                "INSERT",
                _ts("2026-01-01T00:00:00"),
                "s1",
                1,
                1,
                0,
                None,
                None,
                None,
            ),
            (
                1,
                "s-1",
                "B",
                "b",
                "active",
                _ts("2026-01-01T00:00:00"),
                _ts("2026-02-01T00:00:00"),
                "UPDATE",
                _ts("2026-02-01T00:00:00"),
                "s2",
                1,
                2,
                0,
                None,
                None,
                None,
            ),
            (
                1,
                "s-1",
                "C",
                "c",
                "active",
                _ts("2026-01-01T00:00:00"),
                _ts("2026-03-01T00:00:00"),
                "UPDATE",
                _ts("2026-03-01T00:00:00"),
                "s3",
                1,
                3,
                0,
                None,
                None,
                None,
            ),
        ],
        "shop_id int, public_shop_id string, shop_name string, shop_slug string, status string, "
        f"created_at timestamp, updated_at timestamp, {_HISTORY_META_SCHEMA}",
    )

    rows = (
        build_dim_shop_from_history(history, spark, include_unknown=False)
        .orderBy("effective_from")
        .select("shop_name", "effective_from", "effective_to", "is_current")
        .collect()
    )

    assert [row["shop_name"] for row in rows] == ["A", "B", "C"]
    assert rows[0]["effective_to"] == _ts("2026-02-01T00:00:00")
    assert rows[1]["effective_to"] == _ts("2026-03-01T00:00:00")
    assert rows[2]["is_current"] is True


def test_shop_replay_normalizes_batch_timestamptz_before_building_intervals(spark: SparkSession) -> None:
    history = spark.createDataFrame(
        [
            (
                1,
                "s-1",
                "Blue Market",
                "blue-market",
                "active",
                _ts("2026-08-13T10:00:00"),
                _ts("2026-08-13T10:00:00"),
                "INSERT",
                _ts("2026-08-13T03:00:00"),
                "s1",
                1,
                1,
                0,
                None,
                None,
                None,
            ),
            (
                1,
                "s-1",
                "Blue Market Updated",
                "blue-market",
                "active",
                _ts("2026-08-13T10:00:00"),
                _ts("2026-08-13T11:00:00"),
                "UPDATE",
                _ts("2026-08-13T04:00:00"),
                "s2",
                1,
                2,
                0,
                None,
                None,
                None,
            ),
        ],
        "shop_id int, public_shop_id string, shop_name string, shop_slug string, status string, "
        f"created_at timestamp, updated_at timestamp, {_HISTORY_META_SCHEMA}",
    ).withColumn("_ingestion_mode", F.lit("batch"))

    rows = (
        build_dim_shop_from_history(history, spark, include_unknown=False)
        .orderBy("effective_from")
        .select("effective_from", "effective_to")
        .collect()
    )

    assert rows[0]["effective_from"] == _ts("2026-08-13T10:00:00")
    assert rows[0]["effective_to"] == _ts("2026-08-13T11:00:00")
    assert rows[1]["effective_from"] == _ts("2026-08-13T11:00:00")


def test_category_replay_versions_parent_rename(spark: SparkSession) -> None:
    history = spark.createDataFrame(
        [
            (
                1,
                None,
                "Parent A",
                "parent",
                True,
                _ts("2026-01-01T00:00:00"),
                None,
                "INSERT",
                _ts("2026-01-01T00:00:00"),
                "c1",
                1,
                1,
                0,
                None,
                None,
                None,
            ),
            (
                2,
                1,
                "Child",
                "child",
                True,
                _ts("2026-01-02T00:00:00"),
                None,
                "INSERT",
                _ts("2026-01-02T00:00:00"),
                "c2",
                1,
                2,
                0,
                None,
                None,
                None,
            ),
            (
                1,
                None,
                "Parent B",
                "parent",
                True,
                _ts("2026-01-01T00:00:00"),
                _ts("2026-02-01T00:00:00"),
                "UPDATE",
                _ts("2026-02-01T00:00:00"),
                "c3",
                1,
                3,
                0,
                None,
                None,
                None,
            ),
        ],
        "category_id int, parent_category_id int, category_name string, slug string, is_active boolean, "
        f"created_at timestamp, updated_at timestamp, {_HISTORY_META_SCHEMA}",
    )

    rows = (
        build_dim_category_from_history(history, spark, include_unknown=False)
        .where("source_category_id = 2")
        .orderBy("effective_from")
        .select("parent_category_name", "effective_from", "effective_to", "is_current")
        .collect()
    )

    assert [row["parent_category_name"] for row in rows] == ["Parent A", "Parent B"]
    assert rows[0]["effective_to"] == _ts("2026-02-01T00:00:00")
    assert rows[1]["is_current"] is True


def test_product_replay_combines_product_and_variant_timelines(spark: SparkSession) -> None:
    products = spark.createDataFrame(
        [
            (
                10,
                "p-10",
                1,
                2,
                "SKU",
                "slug",
                "Product A",
                "Brand",
                "{}",
                "[]",
                "active",
                True,
                _ts("2026-01-01T00:00:00"),
                None,
                "INSERT",
                _ts("2026-01-01T00:00:00"),
                "p1",
                1,
                1,
                0,
                None,
                None,
                None,
            ),
            (
                10,
                "p-10",
                1,
                2,
                "SKU",
                "slug",
                "Product B",
                "Brand",
                "{}",
                "[]",
                "active",
                True,
                _ts("2026-01-01T00:00:00"),
                _ts("2026-02-01T00:00:00"),
                "UPDATE",
                _ts("2026-02-01T00:00:00"),
                "p2",
                1,
                3,
                0,
                None,
                None,
                None,
            ),
        ],
        "product_id int, public_product_id string, shop_id int, category_id int, product_sku string, "
        "product_slug string, product_name string, brand string, attributes_json string, images_json string, "
        f"status string, is_featured boolean, created_at timestamp, updated_at timestamp, {_HISTORY_META_SCHEMA}",
    )
    variants = spark.createDataFrame(
        [
            (
                100,
                "v-100",
                10,
                "VAR",
                "Default",
                "{}",
                Decimal("100.00"),
                Decimal("120.00"),
                "VND",
                10,
                1,
                Decimal("1.000"),
                "[]",
                "active",
                True,
                _ts("2026-01-02T00:00:00"),
                None,
                "INSERT",
                _ts("2026-01-02T00:00:00"),
                "v1",
                1,
                2,
                0,
                None,
                None,
                None,
            ),
            (
                100,
                "v-100",
                10,
                "VAR",
                "Default",
                "{}",
                Decimal("150.00"),
                Decimal("170.00"),
                "VND",
                5,
                2,
                Decimal("1.000"),
                "[]",
                "active",
                True,
                _ts("2026-01-02T00:00:00"),
                _ts("2026-03-01T00:00:00"),
                "UPDATE",
                _ts("2026-03-01T00:00:00"),
                "v2",
                1,
                4,
                0,
                None,
                None,
                None,
            ),
            (
                100,
                "v-100",
                10,
                "VAR",
                "Premium",
                "{}",
                Decimal("150.00"),
                Decimal("170.00"),
                "VND",
                5,
                2,
                Decimal("1.000"),
                "[]",
                "active",
                True,
                _ts("2026-01-02T00:00:00"),
                _ts("2026-04-01T00:00:00"),
                "UPDATE",
                _ts("2026-04-01T00:00:00"),
                "v3",
                1,
                5,
                0,
                None,
                None,
                None,
            ),
        ],
        "product_variant_id int, public_variant_id string, product_id int, variant_sku string, "
        "variant_name string, options_json string, unit_price decimal(12,2), compare_at_price decimal(12,2), "
        "currency string, stock_quantity int, reserved_quantity int, weight_kg decimal(8,3), images_json string, "
        f"status string, is_default boolean, created_at timestamp, updated_at timestamp, {_HISTORY_META_SCHEMA}",
    )

    rows = (
        build_dim_product_from_history(products, variants, spark, include_unknown=False)
        .orderBy("effective_from")
        .select("product_name", "variant_name", "current_unit_price", "effective_from", "effective_to")
        .collect()
    )

    assert [(row["product_name"], row["variant_name"]) for row in rows] == [
        ("Product A", "Default"),
        ("Product B", "Default"),
        ("Product B", "Premium"),
    ]
    assert rows[1]["current_unit_price"] == Decimal("150.00")
    assert rows[1]["effective_to"] == _ts("2026-04-01T00:00:00")


def test_insert_customer_creates_one_current_scd2_row(spark: SparkSession) -> None:
    result = build_dim_customer_from_history(_history(spark, [{}]), spark, include_unknown=False)

    rows = _business_rows(result)
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    assert rows[0]["is_deleted"] is False


def test_insert_update_closes_old_customer_and_opens_new_version(spark: SparkSession) -> None:
    history = _history(
        spark,
        [
            {},
            {
                "email": "alice.danang@example.com",
                "updated_at": _ts("2026-03-01T10:05:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-03-01T10:05:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            },
        ],
    )

    rows = _business_rows(build_dim_customer_from_history(history, spark, include_unknown=False))
    assert len(rows) == 2
    assert rows[0]["effective_to"] == _ts("2026-03-01T10:05:00")
    assert rows[0]["is_current"] is False
    assert rows[1]["email"] == "alice.danang@example.com"
    assert rows[1]["effective_from"] == _ts("2026-03-01T10:05:00")
    assert rows[1]["is_current"] is True


def test_multiple_updates_in_same_batch_keep_all_customer_versions(spark: SparkSession) -> None:
    history = _history(
        spark,
        [
            {},
            {
                "phone_number": "0900000002",
                "updated_at": _ts("2026-03-01T10:05:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-03-01T10:05:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            },
            {
                "phone_number": "0900000003",
                "updated_at": _ts("2026-03-01T10:10:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-03-01T10:10:00"),
                "_history_event_id": "e3",
                "_source_event_sequence": 3,
            },
        ],
    )

    rows = _business_rows(build_dim_customer_from_history(history, spark, include_unknown=False))
    assert [row["phone_number"] for row in rows] == ["0900000001", "0900000002", "0900000003"]
    assert rows[0]["effective_to"] == _ts("2026-03-01T10:05:00")
    assert rows[1]["effective_to"] == _ts("2026-03-01T10:10:00")
    assert rows[2]["is_current"] is True


def test_delete_closes_current_customer_without_new_version(spark: SparkSession) -> None:
    history = _history(
        spark,
        [
            {},
            {
                "_operation": "DELETE",
                "_event_occurred_at": _ts("2026-04-01T00:00:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            },
        ],
    )

    rows = _business_rows(build_dim_customer_from_history(history, spark, include_unknown=False))
    assert len(rows) == 1
    assert rows[0]["effective_to"] == _ts("2026-04-01T00:00:00")
    assert rows[0]["is_current"] is False
    assert rows[0]["is_deleted"] is True


def test_type1_last_login_update_does_not_create_new_customer_version(spark: SparkSession) -> None:
    history = _history(
        spark,
        [
            {},
            {
                "updated_at": _ts("2026-02-01T00:00:00"),
                "last_login": _ts("2026-02-01T00:00:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-02-01T00:00:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            },
        ],
    )

    rows = _business_rows(build_dim_customer_from_history(history, spark, include_unknown=False))
    assert len(rows) == 1
    assert rows[0]["last_login_at"] == _ts("2026-02-01T00:00:00")
    assert rows[0]["is_current"] is True


def test_customer_reactivation_after_delete_creates_new_active_version(spark: SparkSession) -> None:
    history = _history(
        spark,
        [
            {},  # Insert at 2026-01-01
            {
                "_operation": "DELETE",
                "_event_occurred_at": _ts("2026-04-01T00:00:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            },
            {
                "email": "alice.reactivated@example.com",
                "phone_number": "0999888777",
                "updated_at": _ts("2026-06-01T00:00:00"),
                "_operation": "INSERT",
                "_event_occurred_at": _ts("2026-06-01T00:00:00"),
                "_history_event_id": "e3",
                "_source_event_sequence": 3,
            },
        ],
    )

    rows = _business_rows(build_dim_customer_from_history(history, spark, include_unknown=False))
    assert len(rows) == 2
    # First version was active until deleted at 2026-04-01
    assert rows[0]["effective_to"] == _ts("2026-04-01T00:00:00")
    assert rows[0]["is_current"] is False
    # Second version is active from 2026-06-01 to 9999-12-31
    assert rows[1]["email"] == "alice.reactivated@example.com"
    assert rows[1]["effective_from"] == _ts("2026-06-01T00:00:00")
    assert rows[1]["is_current"] is True
    assert rows[1]["is_deleted"] is False
