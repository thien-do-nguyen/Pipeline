from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from time import sleep
from typing import cast
from zoneinfo import ZoneInfo

import pytest
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

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter
from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig, LakehouseConfig, SparkConfig
from ecommerce_pipeline.control.batch_runs import new_batch_id
from ecommerce_pipeline.control.cloud_lock import PipelineLockUnavailable, cloud_pipeline_lock
from ecommerce_pipeline.control.gold_releases import GoldReleaseStore
from ecommerce_pipeline.control.manifests import BronzeBatchManifest
from ecommerce_pipeline.generator.database import connect
from ecommerce_pipeline.generator.models import SeedPlan
from ecommerce_pipeline.generator.scenarios import seed_continuous, seed_once
from ecommerce_pipeline.ingestion.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.ingestion.streaming.unified_silver import UnifiedSilverMaterializer
from ecommerce_pipeline.pipelines.build_gold import build_gold
from ecommerce_pipeline.pipelines.build_silver import build_silver
from ecommerce_pipeline.runtime.spark import build_spark
from ecommerce_pipeline.validation.batch import validate_batch_lakehouse

_RAW_CDC_SCHEMA = StructType(
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
def test_postgres_to_gold_is_incremental_idempotent_and_reconciled(tmp_path: Path) -> None:
    if os.getenv("RUN_E2E") != "1":
        pytest.skip("Set RUN_E2E=1 with PostgreSQL running")

    seed_once("local", SeedPlan(customers=8, orders=20), seed=42, reset=True)
    base = load_config("local")
    spark_values = {**base.spark.config, "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}
    local_jars = os.getenv("E2E_SPARK_JARS")
    configure_delta_package = local_jars is None
    if local_jars:
        spark_values.pop("spark.jars.packages", None)
        spark_values["spark.jars"] = local_jars
    test_config = base.model_copy(
        update={
            "lakehouse": LakehouseConfig(base_path=str(tmp_path / "lakehouse")),
            "postgres": base.postgres.model_copy(
                update={
                    "max_jdbc_partitions": 4,
                    "target_events_per_partition": 10,
                }
            ),
            "spark": SparkConfig(
                master="local[2]",
                app_name="batch-e2e-test",
                configure_delta_package=configure_delta_package,
                config=spark_values,
            ),
        }
    )
    spark = build_spark(test_config)
    try:
        first_batch_id = new_batch_id(test_config.application.timezone)
        first = extract_all_to_bronze(spark, test_config, first_batch_id)
        assert all(result.record_count > 0 for result in first.results)
        first_silver = build_silver(
            spark,
            test_config,
            batch_id=first_batch_id,
            bronze_manifest=first,
        )
        build_gold(spark, test_config, batch_id=first_batch_id, silver_manifest=first_silver)
        first_report = validate_batch_lakehouse(spark, test_config)

        releases = GoldReleaseStore(spark, test_config)
        release_before = releases.latest()
        assert release_before is not None
        lakehouse = releases.snapshot()
        customer_versions_before = lakehouse.read_table("gold", "dim_customer").count()
        product_versions_before = lakehouse.read_table("gold", "dim_product").count()
        shop_versions_before = lakehouse.read_table("gold", "dim_shop").count()
        category_versions_before = lakehouse.read_table("gold", "dim_category").count()
        fact_before = lakehouse.read_table("gold", "fact_sales")
        fact_rows_before = fact_before.count()
        created_at_row = fact_before.agg({"created_at": "min"}).first()
        assert created_at_row is not None
        created_at_before = created_at_row[0]

        second_batch_id = new_batch_id(test_config.application.timezone)
        second = extract_all_to_bronze(spark, test_config, second_batch_id)
        assert all(result.record_count == 0 for result in second.results)
        second_silver = build_silver(
            spark,
            test_config,
            batch_id=second_batch_id,
            bronze_manifest=second,
        )
        build_gold(spark, test_config, batch_id=second_batch_id, silver_manifest=second_silver)
        assert lakehouse.read_table("gold", "fact_sales").count() == fact_rows_before
        current_created_at_row = lakehouse.read_table("gold", "fact_sales").agg({"created_at": "min"}).first()
        assert current_created_at_row is not None
        assert current_created_at_row[0] == created_at_before
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before

        seed_continuous("local", seed=99, orders_per_batch=2, interval_seconds=0, max_batches=1)
        third_batch_id = new_batch_id(test_config.application.timezone)
        third = extract_all_to_bronze(spark, test_config, third_batch_id)
        changed = {result.table_name: result.record_count for result in third.results}
        assert changed["orders"] == 4  # two inserts, one lifecycle update and one hard delete
        assert changed["app_users"] == 1
        assert changed["user_addresses"] == 1
        assert changed["products"] == 1
        assert changed["product_variants"] == 1
        assert changed["shops"] == 1
        assert changed["categories"] == 1
        assert changed["vouchers"] == 2  # rotate the delete marker
        assert changed["payments"] == 4
        assert changed["shipments"] == 4

        third_silver = build_silver(
            spark,
            test_config,
            batch_id=third_batch_id,
            bronze_manifest=third,
        )
        build_gold(spark, test_config, batch_id=third_batch_id, silver_manifest=third_silver)
        final_report = validate_batch_lakehouse(spark, test_config)
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before
        assert lakehouse.read_table("gold", "fact_sales").count() == fact_rows_before

        release_after = releases.latest()
        assert release_after is not None
        for table_name in ("dim_customer", "dim_product", "dim_shop", "dim_category"):
            assert release_after.gold_versions[table_name] == release_before.gold_versions[table_name] + 1
        assert release_after.gold_versions["fact_sales"] == release_before.gold_versions["fact_sales"] + 2

        lakehouse = releases.snapshot()
        assert final_report.gold.source_order_rows == first_report.gold.source_order_rows + 1
        assert lakehouse.read_table("gold", "dim_customer").count() == customer_versions_before + 1
        assert lakehouse.read_table("gold", "dim_product").count() == product_versions_before + 2
        assert lakehouse.read_table("gold", "dim_shop").count() == shop_versions_before + 1
        assert lakehouse.read_table("gold", "dim_category").count() == category_versions_before + 1
    finally:
        spark.stop()


@pytest.mark.e2e
def test_cdc_streaming_updates_unified_silver_and_gold_idempotently(tmp_path: Path) -> None:
    """Apply a PostgreSQL business change through the CDC Bronze/Silver/Gold path."""

    if os.getenv("RUN_E2E") != "1":
        pytest.skip("Set RUN_E2E=1 with PostgreSQL running")

    seed_once("local", SeedPlan(customers=8, orders=20), seed=42, reset=True)
    base = load_config("local")
    spark_values = {**base.spark.config, "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}
    local_jars = os.getenv("E2E_SPARK_JARS")
    configure_delta_package = local_jars is None
    if local_jars:
        spark_values.pop("spark.jars.packages", None)
        spark_values["spark.jars"] = local_jars
    test_config = base.model_copy(
        update={
            "lakehouse": LakehouseConfig(base_path=str(tmp_path / "lakehouse")),
            "coordination": base.coordination.model_copy(update={"local_lock_path": str(tmp_path / "runtime")}),
            "postgres": base.postgres.model_copy(update={"max_jdbc_partitions": 2, "target_events_per_partition": 10}),
            "spark": SparkConfig(
                master="local[2]",
                app_name="streaming-e2e-test",
                configure_delta_package=configure_delta_package,
                config=spark_values,
            ),
        }
    )
    spark = build_spark(test_config)
    try:
        bootstrap_id = new_batch_id(test_config.application.timezone)
        bootstrap_bronze = extract_all_to_bronze(spark, test_config, bootstrap_id)
        bootstrap_silver = build_silver(
            spark,
            test_config,
            batch_id=bootstrap_id,
            bronze_manifest=bootstrap_bronze,
        )
        build_gold(spark, test_config, batch_id=bootstrap_id, silver_manifest=bootstrap_silver)

        customer_id, source_at, source_payload = _update_source_customer_for_batch(test_config)
        cdc_at = source_at + timedelta(seconds=1)
        source_payload.update(first_name="Streaming Fresh", updated_at=cdc_at)
        raw_cdc = spark.createDataFrame([_customer_cdc_event(source_payload, cdc_at)], _RAW_CDC_SCHEMA)
        materializer = UnifiedSilverMaterializer(spark, test_config)

        materializer.process_batch(raw_cdc, batch_id=7001)
        materializer.process_batch(raw_cdc, batch_id=7001)

        lakehouse = LakehouseAdapter(spark, test_config)
        silver_rows = (
            lakehouse.read_table("silver", "app_users")
            .filter(F.col("user_id") == customer_id)
            .select("first_name", "_ingestion_mode", "_is_deleted")
            .collect()
        )
        assert [row.asDict() for row in silver_rows] == [
            {"first_name": "Streaming Fresh", "_ingestion_mode": "cdc", "_is_deleted": False}
        ]
        current_gold = (
            GoldReleaseStore(spark, test_config)
            .snapshot()
            .read_table("gold", "dim_customer")
            .filter((F.col("source_customer_id") == customer_id) & F.col("is_current"))
            .select("first_name")
            .collect()
        )
        assert [row["first_name"] for row in current_gold] == ["Streaming Fresh"]
        validate_batch_lakehouse(spark, test_config)
    finally:
        spark.stop()


@pytest.mark.e2e
def test_shared_writer_lock_serializes_batch_and_cdc_and_converges(tmp_path: Path) -> None:
    """Exercise the real Delta lock, then race Batch and CDC shared-layer writers."""

    if os.getenv("RUN_E2E") != "1":
        pytest.skip("Set RUN_E2E=1 with PostgreSQL running")

    seed_once("local", SeedPlan(customers=4, orders=6), seed=314, reset=True)
    base = load_config("local")
    spark_values = {**base.spark.config, "spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"}
    local_jars = os.getenv("E2E_SPARK_JARS")
    configure_delta_package = local_jars is None
    if local_jars:
        spark_values.pop("spark.jars.packages", None)
        spark_values["spark.jars"] = local_jars
    test_config = base.model_copy(
        update={
            "lakehouse": LakehouseConfig(base_path=str(tmp_path / "lakehouse")),
            "coordination": base.coordination.model_copy(update={"local_lock_path": str(tmp_path / "runtime")}),
            "postgres": base.postgres.model_copy(update={"max_jdbc_partitions": 2, "target_events_per_partition": 10}),
            "spark": SparkConfig(
                master="local[2]",
                app_name="batch-cdc-concurrency-e2e-test",
                configure_delta_package=configure_delta_package,
                config=spark_values,
            ),
        }
    )
    spark = build_spark(test_config)
    try:
        # Use the cloud writer path against a local Delta table. This exercises
        # the same optimistic Delta lock used by independent Databricks Jobs,
        # without mocking its acquire, wait, timeout or release operations.
        shared_writer_config = test_config.model_copy(
            update={
                "spark": test_config.spark.model_copy(update={"master": None}),
                "coordination": test_config.coordination.model_copy(update={"lock_wait_seconds": 60}),
            }
        )
        _assert_shared_delta_lock_protocol(spark, shared_writer_config)

        bootstrap_id = new_batch_id(test_config.application.timezone)
        bootstrap_bronze = extract_all_to_bronze(spark, test_config, bootstrap_id)
        bootstrap_silver = build_silver(
            spark,
            test_config,
            batch_id=bootstrap_id,
            bronze_manifest=bootstrap_bronze,
        )
        build_gold(
            spark,
            test_config,
            batch_id=bootstrap_id,
            silver_manifest=bootstrap_silver,
        )

        customer_id, batch_at, source_payload = _update_source_customer_for_batch(test_config)
        cdc_at = batch_at + timedelta(seconds=1)
        source_payload.update(first_name="CDC Wins", updated_at=cdc_at)
        raw_cdc = spark.createDataFrame(
            [_customer_cdc_event(source_payload, cdc_at)],
            _RAW_CDC_SCHEMA,
        )

        batch_bronze_committed = Event()
        start_shared_writers = Event()
        state: dict[str, BronzeBatchManifest] = {}

        def run_staged_batch() -> None:
            batch_id = new_batch_id(test_config.application.timezone)
            try:
                manifest = extract_all_to_bronze(spark, test_config, batch_id)
                state["batch_manifest"] = manifest
            finally:
                batch_bronze_committed.set()
            if not start_shared_writers.wait(timeout=300):
                raise TimeoutError("CDC writer did not reach the shared-layer boundary")
            manifest = state["batch_manifest"]
            with cloud_pipeline_lock(spark, shared_writer_config, f"batch-e2e-{batch_id}"):
                silver_manifest = build_silver(
                    spark,
                    test_config,
                    batch_id=batch_id,
                    bronze_manifest=manifest,
                )
                build_gold(
                    spark,
                    test_config,
                    batch_id=batch_id,
                    silver_manifest=silver_manifest,
                )

        def run_cdc() -> None:
            if not batch_bronze_committed.wait(timeout=300):
                raise TimeoutError("Batch Bronze did not reach its orchestration boundary")
            manifest = state.get("batch_manifest")
            if manifest is None:
                raise RuntimeError("Batch Bronze failed before publishing its manifest")
            app_users = next(result for result in manifest.results if result.table_name == "app_users")
            assert app_users.record_count == 1
            start_shared_writers.set()
            UnifiedSilverMaterializer(spark, shared_writer_config).process_batch(raw_cdc, batch_id=9001)

        with ThreadPoolExecutor(max_workers=2) as executor:
            batch_future = executor.submit(run_staged_batch)
            cdc_future = executor.submit(run_cdc)
            batch_future.result(timeout=900)
            cdc_future.result(timeout=900)

        lakehouse = LakehouseAdapter(spark, test_config)
        silver_customers = (
            lakehouse.read_table("silver", "app_users")
            .filter(f"user_id = {customer_id}")
            .select("first_name", "_ingestion_mode", "_is_deleted")
            .collect()
        )
        assert len(silver_customers) == 1
        silver_customer = silver_customers[0]
        assert silver_customer.asDict() == {
            "first_name": "CDC Wins",
            "_ingestion_mode": "cdc",
            "_is_deleted": False,
        }

        current_gold = (
            GoldReleaseStore(spark, test_config)
            .snapshot()
            .read_table("gold", "dim_customer")
            .filter(f"source_customer_id = {customer_id} AND is_current")
            .select("first_name")
            .collect()
        )
        assert [row["first_name"] for row in current_gold] == ["CDC Wins"]

        assert (
            lakehouse.read_table("silver", "app_users").groupBy("user_id").count().filter(F.col("count") > 1).count()
            == 0
        )
        assert (
            GoldReleaseStore(spark, test_config)
            .snapshot()
            .read_table("gold", "fact_sales")
            .groupBy("source_order_id", "source_order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
            == 0
        )
        validate_batch_lakehouse(spark, test_config)
    finally:
        spark.stop()


def _assert_shared_delta_lock_protocol(spark: SparkSession, config: AppConfig) -> None:
    """Prove acquire/wait/release/acquire and timeout with the real Delta lock."""

    holder_acquired = Event()
    release_holder = Event()
    waiter_attempted = Event()
    order: list[str] = []

    def holder() -> None:
        with cloud_pipeline_lock(spark, config, "batch-writer"):
            order.append("batch.acquire")
            holder_acquired.set()
            if not release_holder.wait(timeout=60):
                raise TimeoutError("Test did not release the batch writer")
            order.append("batch.release")

    def waiter() -> None:
        waiter_attempted.set()
        with cloud_pipeline_lock(spark, config, "streaming-writer"):
            order.append("streaming.acquire")
        order.append("streaming.release")

    with ThreadPoolExecutor(max_workers=2) as executor:
        holder_future = executor.submit(holder)
        assert holder_acquired.wait(timeout=60)

        timeout_config = config.model_copy(
            update={"coordination": config.coordination.model_copy(update={"lock_wait_seconds": 0})}
        )
        with (
            pytest.raises(PipelineLockUnavailable, match="Timed out waiting for cloud pipeline lock"),
            cloud_pipeline_lock(spark, timeout_config, "timeout-writer"),
        ):
            pass

        waiter_future = executor.submit(waiter)
        assert waiter_attempted.wait(timeout=10)
        sleep(1)
        assert not waiter_future.done(), "Streaming writer bypassed the active batch lock"

        release_holder.set()
        holder_future.result(timeout=60)
        waiter_future.result(timeout=60)

    assert order == [
        "batch.acquire",
        "batch.release",
        "streaming.acquire",
        "streaming.release",
    ]


def _update_source_customer_for_batch(config: AppConfig) -> tuple[int, datetime, dict[str, object]]:
    changed_at = datetime.now(ZoneInfo(config.application.timezone)).replace(tzinfo=None, microsecond=0)
    with connect(config.postgres) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE app_users
            SET first_name = 'Batch Stale', updated_at = %s
            WHERE user_id = (SELECT MIN(user_id) FROM app_users)
            RETURNING user_id, public_user_id, username, email, first_name, last_name,
                      phone_number, status, created_at, updated_at, last_login
            """,
            (changed_at,),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("E2E source does not contain a customer")
        connection.commit()
    typed_row = cast(dict[str, object], row)
    return cast(int, typed_row["user_id"]), changed_at, typed_row


def _customer_cdc_event(payload: dict[str, object], occurred_at: datetime) -> Row:
    topic = "ecommerce.domain.customer"
    return Row(
        _transport_event_id=f"{topic}:0:9001",
        topic=topic,
        partition=0,
        offset=9001,
        value_json=json.dumps({"before": None, "after": payload}, default=str),
        source_schema="customer_app",
        source_table="app_users",
        operation="u",
        source_lsn=9_000_001,
        source_tx_id=9001,
        transaction_id="9001:9000001",
        transaction_order=1,
        collection_order=1,
        event_timestamp=occurred_at,
        is_valid=True,
        parse_error=None,
        ingested_at=occurred_at,
    )
