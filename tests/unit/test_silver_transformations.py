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
        "_batch_id",
        "_operation",
    ]
    bronze = spark.createDataFrame(rows, columns)

    result = transform_customer_table(bronze, get_silver_contract("app_users")).first()

    assert result is not None
    assert result["email"] == "new@example.com"
    assert result["username"] == "user01"
    assert result["phone_number"] == "+84901112222"
    assert result["_bronze_event_id"] == 20
    assert result["_bronze_batch_id"] == "batch-2"


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
        "_batch_id",
        "_operation",
    ]
    bronze = spark.createDataFrame(rows, columns)

    result = transform_customer_table(bronze, get_silver_contract("app_users")).first()

    assert result is not None
    assert result["_operation"] == "DELETE"
    assert result["_bronze_event_id"] == 20
    assert result["_bronze_batch_id"] == "batch-2"
