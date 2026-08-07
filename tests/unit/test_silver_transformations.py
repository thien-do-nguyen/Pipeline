from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession

from ecommerce_pipeline.contracts.silver_tables import get_silver_contract
from ecommerce_pipeline.transformations.silver.customers import transform_customer_table


def test_silver_keeps_latest_source_version_and_normalizes_customer(spark: SparkSession) -> None:
    rows = [
        (
            1,
            "uuid-1",
            " USER01 ",
            " OLD@EXAMPLE.COM ",
            "Old",
            "Name",
            "090-111-2222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 1, 10),
            datetime(2026, 1, 1, 9),
            "hash-old",
            10,
            datetime(2026, 1, 1, 10),
            "batch-1",
            "INSERT",
        ),
        (
            1,
            "uuid-1",
            " USER01 ",
            " NEW@EXAMPLE.COM ",
            "New",
            "Name",
            "+84 901 112 222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 2, 10),
            datetime(2026, 1, 2, 9),
            "hash-new",
            20,
            datetime(2026, 1, 2, 10),
            "batch-2",
            "UPDATE",
        ),
    ]
    columns = [
        "user_id",
        "public_user_id",
        "username",
        "email",
        "first_name",
        "last_name",
        "phone_number",
        "status",
        "created_at",
        "updated_at",
        "last_login",
        "_record_hash",
        "_event_id",
        "_event_occurred_at",
        "_batch_id",
        "_operation",
    ]
    bronze = spark.createDataFrame(rows, columns)

    result = transform_customer_table(bronze, get_silver_contract("app_users")).first()

    assert result is not None
    assert result["email"] == "new@example.com"
    assert result["username"] == "user01"
    assert result["phone_number"] == "+84901112222"
    assert result["_bronze_event_id"] == "20"
    assert result["_bronze_batch_id"] == "batch-2"
    assert result["_ingestion_mode"] == "batch"
    assert result["_is_deleted"] is False


def test_silver_keeps_delete_tombstone_when_it_is_latest_source_version(spark: SparkSession) -> None:
    rows = [
        (
            1,
            "uuid-1",
            " USER01 ",
            " OLD@EXAMPLE.COM ",
            "Old",
            "Name",
            "090-111-2222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 1, 10),
            datetime(2026, 1, 1, 9),
            "hash-old",
            10,
            datetime(2026, 1, 1, 10),
            "batch-1",
            "INSERT",
        ),
        (
            1,
            "uuid-1",
            " USER01 ",
            " OLD@EXAMPLE.COM ",
            "Old",
            "Name",
            "090-111-2222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 1, 10),
            datetime(2026, 1, 1, 9),
            "hash-delete",
            20,
            datetime(2026, 1, 2, 10),
            "batch-2",
            "DELETE",
        ),
    ]
    columns = [
        "user_id",
        "public_user_id",
        "username",
        "email",
        "first_name",
        "last_name",
        "phone_number",
        "status",
        "created_at",
        "updated_at",
        "last_login",
        "_record_hash",
        "_event_id",
        "_event_occurred_at",
        "_batch_id",
        "_operation",
    ]
    bronze = spark.createDataFrame(rows, columns)

    result = transform_customer_table(bronze, get_silver_contract("app_users")).first()

    assert result is not None
    assert result["_operation"] == "DELETE"
    assert result["_bronze_event_id"] == "20"
    assert result["_bronze_batch_id"] == "batch-2"
    assert result["_is_deleted"] is True


def test_silver_prefers_cdc_when_batch_and_cdc_have_the_same_event_time(spark: SparkSession) -> None:
    rows = [
        (
            1,
            "uuid-1",
            "user01",
            "batch@example.com",
            "Batch",
            "Source",
            "0901112222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 2),
            datetime(2026, 1, 1),
            "batch-hash",
            "999",
            datetime(2026, 1, 2),
            "batch-1",
            "UPDATE",
            "batch_change_event",
            None,
            None,
        ),
        (
            1,
            "uuid-1",
            "user01",
            "cdc@example.com",
            "CDC",
            "Source",
            "0901112222",
            "ACTIVE",
            datetime(2025, 1, 1),
            datetime(2026, 1, 2),
            datetime(2026, 1, 1),
            "cdc-hash",
            "ecommerce.customer_app.app_users:0:4",
            datetime(2026, 1, 2),
            "stream-4",
            "UPDATE",
            "streaming_cdc",
            1234,
            4,
        ),
    ]
    columns = [
        "user_id",
        "public_user_id",
        "username",
        "email",
        "first_name",
        "last_name",
        "phone_number",
        "status",
        "created_at",
        "updated_at",
        "last_login",
        "_record_hash",
        "_event_id",
        "_event_occurred_at",
        "_batch_id",
        "_operation",
        "_ingestion_type",
        "_source_lsn",
        "_kafka_offset",
    ]

    result = transform_customer_table(
        spark.createDataFrame(rows, columns),
        get_silver_contract("app_users"),
    ).first()

    assert result is not None
    assert result["email"] == "cdc@example.com"
    assert result["_ingestion_mode"] == "cdc"
    assert result["_ingestion_priority"] == 2
    assert result["_source_event_sequence"] == 1234
    assert result["_source_event_subsequence"] == 4
