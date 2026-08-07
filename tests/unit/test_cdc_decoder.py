from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES
from ecommerce_pipeline.contracts.cdc_tables import (
    TYPED_CDC_TABLES,
    get_typed_cdc_contract,
)
from ecommerce_pipeline.ingestion.streaming import cdc_decoder

RAW_SCHEMA = StructType(
    [
        StructField("_transport_event_id", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("value_json", StringType()),
        StructField("source_schema", StringType()),
        StructField("source_table", StringType()),
        StructField("operation", StringType()),
        StructField("source_lsn", LongType()),
        StructField("source_tx_id", LongType()),
        StructField("transaction_id", StringType()),
        StructField("transaction_order", LongType()),
        StructField("collection_order", LongType()),
        StructField("event_timestamp", TimestampType()),
        StructField("is_valid", BooleanType()),
        StructField("parse_error", StringType()),
        StructField("ingested_at", TimestampType()),
    ]
)


def _raw_row(
    payload: dict[str, object],
    *,
    table_name: str = "orders",
    operation: str = "c",
    offset: int = 8,
) -> Row:
    envelope = {
        "before": payload if operation == "d" else None,
        "after": None if operation == "d" else payload,
    }
    topic = f"ecommerce.customer_app.{table_name}"
    return Row(
        _transport_event_id=f"{topic}:1:{offset}",
        topic=topic,
        partition=1,
        offset=offset,
        value_json=json.dumps(envelope),
        source_schema="customer_app",
        source_table=table_name,
        operation=operation,
        source_lsn=123_456,
        source_tx_id=91,
        transaction_id="91:123456",
        transaction_order=2,
        collection_order=1,
        event_timestamp=datetime(2026, 8, 3, 10, 0),
        is_valid=True,
        parse_error=None,
        ingested_at=datetime(2026, 8, 3, 10, 0),
    )


def test_typed_contract_covers_all_sources_without_password_hash() -> None:
    assert set(TYPED_CDC_TABLES) == set(BRONZE_TABLES)
    assert "password_hash" not in get_typed_cdc_contract("app_users").columns
    assert "shipping_address_snapshot" in get_typed_cdc_contract("shipments").columns


def test_create_event_becomes_typed_row(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [
            _raw_row(
                {
                    "order_id": 42,
                    "total_amount": "125.50",
                    "created_at": 1_783_127_082_250_070,
                }
            )
        ],
        RAW_SCHEMA,
    )
    contract = get_typed_cdc_contract("orders")
    prepared = cdc_decoder.prepare_raw_cdc_events(raw)

    row = (
        cdc_decoder.decode_table_events(
            prepared,
            contract,
            batch_id=7,
            query_name="cdc-to-silver",
            source_system="ecommerce",
            timezone="Asia/Ho_Chi_Minh",
        )
        .select(
            "order_id",
            "total_amount",
            F.date_format("created_at", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("created_at"),
            "_event_id",
            "_operation",
            "_record_hash",
            "_batch_id",
        )
        .first()
    )

    assert row is not None
    assert row["order_id"] == 42
    assert row["total_amount"] == Decimal("125.50")
    assert row["created_at"] == "2026-07-04 01:04:42.250070"
    assert row["_event_id"] == "ecommerce.customer_app.orders:1:8"
    assert row["_operation"] == "INSERT"
    assert len(row["_record_hash"]) == 64
    assert row["_batch_id"] == "cdc-to-silver:v1:7"
    assert (
        "_ingestion_type"
        in cdc_decoder.decode_table_events(
            prepared,
            contract,
            batch_id=7,
            query_name="cdc-to-silver",
            source_system="ecommerce",
            timezone="Asia/Ho_Chi_Minh",
        ).columns
    )


def test_delete_event_uses_before_image(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [_raw_row({"user_id": 1001}, table_name="app_users", operation="d")],
        RAW_SCHEMA,
    )
    contract = get_typed_cdc_contract("app_users")

    row = cdc_decoder.decode_table_events(
        cdc_decoder.prepare_raw_cdc_events(raw),
        contract,
        batch_id=4,
        query_name="cdc-to-silver",
        source_system="ecommerce",
        timezone="Asia/Ho_Chi_Minh",
    ).first()

    assert row is not None
    assert row["user_id"] == 1001
    assert row["_operation"] == "DELETE"


def test_schema_drift_is_marked_before_any_target_write(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [_raw_row({"order_id": 42, "new_unmapped_column": "value"})],
        RAW_SCHEMA,
    )

    error = cdc_decoder.prepare_raw_cdc_events(raw).select("_decode_error").first()

    assert error is not None
    assert error["_decode_error"] == "unexpected_payload_fields:new_unmapped_column"


def test_invalid_business_type_is_marked_for_quarantine(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [_raw_row({"order_id": 42, "total_amount": "not-a-decimal"})],
        RAW_SCHEMA,
    )

    error = cdc_decoder.prepare_raw_cdc_events(raw).select("_decode_error").first()

    assert error is not None
    assert error["_decode_error"] == "invalid_field_type:total_amount"
