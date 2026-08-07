from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ecommerce_pipeline.transformations.gold.scd2 import (
    build_dim_customer_from_history,
    build_dim_customer_incremental,
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


def _apply_upserts(existing: DataFrame, upserts: DataFrame) -> DataFrame:
    return existing.join(upserts.select("customer_key"), "customer_key", "left_anti").unionByName(upserts)


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


def test_retry_same_incremental_batch_is_idempotent_by_customer_key(spark: SparkSession) -> None:
    initial = build_dim_customer_from_history(_history(spark, [{}]), spark, include_unknown=False)
    batch = _history(
        spark,
        [
            {
                "email": "alice.retry@example.com",
                "updated_at": _ts("2026-03-01T10:05:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-03-01T10:05:00"),
                "_history_event_id": "e2",
                "_source_event_sequence": 2,
            }
        ],
    )
    upserts = build_dim_customer_incremental(initial, batch, spark)

    once = _apply_upserts(initial, upserts)
    twice = _apply_upserts(once, upserts)

    assert once.select("customer_key").distinct().count() == once.count()
    assert twice.select("customer_key").distinct().count() == twice.count()
    assert _business_rows(once) == _business_rows(twice)


def test_full_rebuild_matches_incremental_customer_processing(spark: SparkSession) -> None:
    all_history = _history(
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
                "updated_at": _ts("2026-03-02T00:00:00"),
                "last_login": _ts("2026-03-02T00:00:00"),
                "_operation": "UPDATE",
                "_event_occurred_at": _ts("2026-03-02T00:00:00"),
                "_history_event_id": "e3",
                "_source_event_sequence": 3,
            },
        ],
    )
    initial = build_dim_customer_from_history(
        all_history.where(F.col("_source_event_sequence") == 1), spark, include_unknown=False
    )
    upserts = build_dim_customer_incremental(
        initial,
        all_history.where(F.col("_source_event_sequence") > 1),
        spark,
    )
    incremental = _apply_upserts(initial, upserts)
    full = build_dim_customer_from_history(all_history, spark, include_unknown=False)

    assert _business_rows(incremental) == _business_rows(full)
