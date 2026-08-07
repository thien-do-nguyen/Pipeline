from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.adapters.lakehouse import read_delta
from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import LakehouseConfig, SparkConfig
from ecommerce_pipeline.contracts.cdc_tables import TYPED_CDC_TABLES
from ecommerce_pipeline.ingestion.streaming.cdc_decoder import prepare_raw_cdc_events
from ecommerce_pipeline.ingestion.streaming.typed_bronze import TypedBronzeMaterializer
from ecommerce_pipeline.runtime.spark import build_spark

_RAW_SCHEMA = StructType(
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


@pytest.mark.e2e
def test_typed_bronze_is_physical_quarantined_and_idempotent(tmp_path: Path) -> None:
    base = load_config("local")
    spark_config = {
        **base.spark.config,
        "spark.sql.shuffle.partitions": "2",
        "spark.ui.enabled": "false",
    }
    config = base.model_copy(
        update={
            "lakehouse": LakehouseConfig(base_path=str(tmp_path / "lakehouse")),
            "spark": SparkConfig(
                master="local[2]",
                app_name="typed-bronze-e2e-test",
                configure_delta_package=True,
                config=spark_config,
            ),
        }
    )
    spark = build_spark(config)
    try:
        now = datetime(2026, 8, 5, 10, 0)
        rows = [
            _raw_event(1, {"order_id": 42, "total_amount": "125.50"}, now),
            _raw_event(2, {"order_id": 43, "total_amount": "bad-decimal"}, now),
        ]
        prepared = prepare_raw_cdc_events(spark.createDataFrame(rows, _RAW_SCHEMA)).cache()
        materializer = TypedBronzeMaterializer(spark, config)
        try:
            first = materializer.materialize(prepared, 7)
            retry = materializer.materialize(prepared, 7)
        finally:
            prepared.unpersist()

        assert first.quarantined_count == 1
        assert retry.quarantined_count == 1
        for table_name in TYPED_CDC_TABLES:
            reference = config.lakehouse.streaming_typed_bronze_reference(table_name)
            assert DeltaTable.isDeltaTable(spark, reference.value)

        orders = read_delta(spark, config.lakehouse.streaming_typed_bronze_reference("orders"))
        quarantine = read_delta(
            spark,
            config.lakehouse.streaming_typed_bronze_reference("quarantine"),
        )
        assert orders.count() == 1
        assert quarantine.count() == 1
        assert quarantine.first()["_quarantine_reason"] == "invalid_field_type:total_amount"
    finally:
        spark.stop()


def _raw_event(offset: int, payload: dict[str, object], timestamp: datetime) -> Row:
    topic = "ecommerce.customer_app.orders"
    return Row(
        _transport_event_id=f"{topic}:0:{offset}",
        topic=topic,
        partition=0,
        offset=offset,
        value_json=json.dumps({"before": None, "after": payload}),
        source_schema="customer_app",
        source_table="orders",
        operation="c",
        source_lsn=100 + offset,
        source_tx_id=10,
        transaction_id="10:100",
        transaction_order=offset,
        collection_order=1,
        event_timestamp=timestamp,
        is_valid=True,
        parse_error=None,
        ingested_at=timestamp,
    )
