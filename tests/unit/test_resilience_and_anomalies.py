from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.contracts.cdc_tables import get_typed_cdc_contract
from ecommerce_pipeline.ingestion.streaming import cdc_decoder
from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_location,
    location_hash_from_json,
    natural_hash,
    positive_hash_key,
)

RAW_SCHEMA = StructType(
    [
        StructField("_transport_event_id", StringType()),
        StructField("topic", StringType()),
        StructField("partition", LongType()),
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


def test_unicode_and_sql_injection_resilience(spark: SparkSession) -> None:
    """Ensure natural hashing gracefully handles emojis, Vietnamese diacritics, and SQL injection strings."""
    malicious_inputs = [
        "Robert'); DROP TABLE orders;--",
        "Nguyễn Văn Ánh 🛍️ ✨",
        "<script>alert('xss')</script>",
        "Line 1\nLine 2\tTabbed",
        "NULL",
        "",
    ]
    df = spark.createDataFrame([(i, val) for i, val in enumerate(malicious_inputs)], "id int, text string")
    hashed = df.withColumn("hash", natural_hash(F.col("text"))).withColumn("key", positive_hash_key(F.col("hash")))

    rows = hashed.collect()
    assert len(rows) == len(malicious_inputs)
    # All hashes must be valid 64-character SHA256 hex strings
    for r in rows:
        assert len(r["hash"]) == 64
        assert r["key"] >= 0


def test_corrupt_and_empty_json_snapshots_handling(spark: SparkSession) -> None:
    """Ensure malformed, empty, or non-json address snapshots do not crash location extraction."""
    addresses = spark.createDataFrame(
        [],
        """address_id int, user_id int, address_type string, recipient_name string,
           phone_number string, street string, ward string, district string, city string,
           state string, postal_code string, country string, is_default boolean,
           created_at timestamp, updated_at timestamp""",
    )
    orders = spark.createDataFrame(
        [
            (1, 10, 100, 101, "", None),  # Empty string & None
            (2, 11, 102, 103, "{broken json", "{invalid:"),  # Malformed JSON
            (3, 12, 104, 105, json.dumps({"street": "123 Le Loi"}), json.dumps({"city": "Hanoi"})),
        ],
        """order_id int, customer_id int, shipping_address_id int, billing_address_id int,
           shipping_address_snapshot string, billing_address_snapshot string""",
    )

    # location_hash_from_json should safely produce hashes without throwing JVM/JSON parsing exceptions
    test_df = orders.withColumn("ship_hash", location_hash_from_json(F.col("shipping_address_snapshot")))
    assert test_df.count() == 3

    # build_dim_location should extract and handle gracefully
    dim_location = build_dim_location(addresses, orders, spark)
    collected = dim_location.collect()
    assert len(collected) >= 4  # 1 unknown + valid snapshot rows


def test_cdc_decoder_handles_tombstone_and_delete_payloads(spark: SparkSession) -> None:
    """Ensure streaming CDC decoder handles tombstone events (delete with null after) correctly."""
    delete_row = Row(
        _transport_event_id="topic:1:10",
        topic="topic",
        partition=1,
        offset=10,
        value_json=json.dumps(
            {
                "before": {"user_id": 1, "username": "alice", "email": "alice@example.com"},
                "after": None,
            }
        ),
        source_schema="customer_app",
        source_table="app_users",
        operation="d",
        source_lsn=1000,
        source_tx_id=1,
        transaction_id="1:1000",
        transaction_order=1,
        collection_order=1,
        event_timestamp=datetime(2026, 8, 1, 10, 0),
        is_valid=True,
        parse_error=None,
        ingested_at=datetime(2026, 8, 1, 10, 0),
    )
    raw = spark.createDataFrame([delete_row], RAW_SCHEMA)
    prepared = cdc_decoder.prepare_raw_cdc_events(raw)
    decoded = cdc_decoder.decode_table_events(
        prepared,
        get_typed_cdc_contract("app_users"),
        batch_id=1,
        query_name="cdc-to-silver",
        source_system="ecommerce",
        timezone="Asia/Ho_Chi_Minh",
    )
    first = decoded.first()
    assert first is not None
    assert first["_operation"] == "DELETE"
    assert first["user_id"] == 1
    assert first["username"] == "alice"
