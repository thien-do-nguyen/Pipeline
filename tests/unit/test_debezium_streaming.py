from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import Mock, call

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.config.models import KafkaSourceConfig
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES
from ecommerce_pipeline.contracts.cdc_routing import CDC_DOMAIN_TABLES
from ecommerce_pipeline.ingestion.streaming.debezium import normalize_debezium_events
from ecommerce_pipeline.ingestion.streaming.kafka_source import read_kafka_stream

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField(
            "headers",
            ArrayType(StructType([StructField("key", StringType()), StructField("value", BinaryType())])),
        ),
    ]
)


def _kafka_row(value: str | None, *, offset: int = 7) -> Row:
    return Row(
        key=b'{"order_id":42}',
        value=None if value is None else value.encode(),
        topic="ecommerce.domain.sales",
        partition=2,
        offset=offset,
        timestamp=None,
        headers=[],
    )


def test_debezium_event_is_normalized_without_losing_raw_payload(spark: SparkSession) -> None:
    payload = {
        "before": None,
        "after": {"order_id": 42, "total_amount": "125.50"},
        "source": {
            "schema": "customer_app",
            "table": "orders",
            "lsn": 123456,
            "txId": 91,
            "ts_ms": 1_720_000_000_000,
            "snapshot": "false",
        },
        "transaction": {"id": "91:123456", "total_order": 2, "data_collection_order": 1},
        "op": "c",
        "ts_ms": 1_720_000_000_010,
    }

    row = normalize_debezium_events(
        spark.createDataFrame([_kafka_row(json.dumps(payload))], schema=KAFKA_SCHEMA)
    ).first()

    assert row is not None
    assert row["_transport_event_id"] == "ecommerce.domain.sales:2:7"
    assert row["source_schema"] == "customer_app"
    assert row["source_table"] == "orders"
    assert row["operation"] == "c"
    assert row["source_lsn"] == 123456
    assert row["is_valid"] is True
    assert row["parse_error"] is None
    assert json.loads(row["value_json"])["after"]["total_amount"] == "125.50"


def test_null_kafka_value_is_retained_as_invalid_event(spark: SparkSession) -> None:
    row = normalize_debezium_events(spark.createDataFrame([_kafka_row(None)], schema=KAFKA_SCHEMA)).first()

    assert row is not None
    assert row["is_valid"] is False
    assert row["parse_error"] == "tombstone_or_null_value"


def test_transport_event_id_includes_cloud_source_epoch(spark: SparkSession) -> None:
    row = normalize_debezium_events(
        spark.createDataFrame([_kafka_row(None)], schema=KAFKA_SCHEMA),
        source_id="tk1-ecommerce-cdc-dev-a1b2c3",
    ).first()

    assert row is not None
    assert row["_transport_event_id"] == ("tk1-ecommerce-cdc-dev-a1b2c3:ecommerce.domain.sales:2:7")


def test_kafka_source_uses_only_protocol_options() -> None:
    spark = Mock()
    reader = spark.readStream.format.return_value
    reader.option.return_value = reader
    expected = Mock()
    reader.load.return_value = expected
    config = KafkaSourceConfig(
        bootstrap_servers="localhost:29092",
        topic_pattern=r"ecommerce\.domain\..*",
        max_offsets_per_trigger=250,
        options={"kafka.security.protocol": "PLAINTEXT"},
    )

    assert read_kafka_stream(spark, config) is expected
    assert call("subscribePattern", r"ecommerce\.domain\..*") in reader.option.call_args_list
    assert call("maxOffsetsPerTrigger", "250") in reader.option.call_args_list
    assert call("kafka.security.protocol", "PLAINTEXT") in reader.option.call_args_list


def test_connector_captures_exactly_the_declared_bronze_sources() -> None:
    path = Path(__file__).resolve().parents[2] / "infra/local/connect/postgres-cdc.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    actual = {table.removeprefix("customer_app.") for table in config["table.include.list"].split(",")}

    assert actual == set(BRONZE_TABLES)
    assert config["column.exclude.list"] == "customer_app.app_users.password_hash"
    transforms = config["transforms"].split(",")
    assert set(transforms) == {f"route{domain}" for domain in CDC_DOMAIN_TABLES}
    for domain, tables in CDC_DOMAIN_TABLES.items():
        transform = f"route{domain}"
        topic_regex = re.compile(config[f"transforms.{transform}.topic.regex"])
        assert config[f"transforms.{transform}.topic.replacement"] == f"ecommerce.domain.{domain}"
        assert config[f"transforms.{transform}.key.enforce.uniqueness"] == "true"
        assert all(topic_regex.fullmatch(f"ecommerce.customer_app.{table}") for table in tables)
        assert not any(
            topic_regex.fullmatch(f"ecommerce.customer_app.{table}")
            for other_domain, other_tables in CDC_DOMAIN_TABLES.items()
            if other_domain != domain
            for table in other_tables
        )
    assert config["topic.prefix"] == "ecommerce"
    assert config["publication.autocreate.mode"] == "disabled"


def test_local_kafka_bootstrap_creates_one_topic_per_domain() -> None:
    path = Path(__file__).resolve().parents[2] / "infra/local/kafka/create-topics.sh"
    script = path.read_text(encoding="utf-8")

    for domain in CDC_DOMAIN_TABLES:
        assert domain in script
    assert 'create_topic "ecommerce.domain.${domain}" 1 "delete"' in script
    assert "ecommerce.customer_app.${table_name}" not in script
