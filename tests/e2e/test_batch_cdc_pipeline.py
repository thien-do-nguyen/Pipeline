from __future__ import annotations

import json
import os
from argparse import Namespace
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, Lock
from time import monotonic, sleep
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
from ecommerce_pipeline.generator.database import connect
from ecommerce_pipeline.generator.models import SeedPlan
from ecommerce_pipeline.generator.scenarios import seed_continuous, seed_once
from ecommerce_pipeline.ingestion.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.ingestion.streaming import unified_silver as unified_silver_module
from ecommerce_pipeline.ingestion.streaming.unified_silver import UnifiedSilverMaterializer
from ecommerce_pipeline.jobs import run_batch as run_batch_job
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
def test_shared_writer_lock_serializes_batch_and_cdc_and_converges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Race the production Batch and Streaming stage-level lock boundaries."""

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

        lock_observer = _ProductionLockObserver(cloud_pipeline_lock)
        monkeypatch.setattr(run_batch_job, "cloud_pipeline_lock", lock_observer.lock)
        monkeypatch.setattr(unified_silver_module, "cloud_pipeline_lock", lock_observer.lock)
        start_writers = Barrier(2)
        batch_args = Namespace(
            mode="all",
            tables=None,
            full_rebuild_silver=False,
            full_rebuild_gold=False,
        )

        def run_batch() -> None:
            start_writers.wait(timeout=60)
            run_batch_job.run_mode(
                spark,
                batch_args,
                shared_writer_config,
                "batch-concurrency",
                {},
            )

        def run_cdc() -> None:
            start_writers.wait(timeout=60)
            UnifiedSilverMaterializer(spark, shared_writer_config).process_batch(raw_cdc, batch_id=9001)

        with ThreadPoolExecutor(max_workers=2) as executor:
            batch_future = executor.submit(run_batch)
            cdc_future = executor.submit(run_cdc)
            assert lock_observer.batch_silver_acquired.wait(timeout=300)
            assert lock_observer.streaming_silver_attempted.wait(timeout=300)
            sleep(1)

            timeout_config = shared_writer_config.model_copy(
                update={"coordination": shared_writer_config.coordination.model_copy(update={"lock_wait_seconds": 0})}
            )
            with (
                pytest.raises(PipelineLockUnavailable, match="Timed out waiting for cloud pipeline lock"),
                cloud_pipeline_lock(spark, timeout_config, "timeout-writer"),
            ):
                pass

            assert not lock_observer.has_acquired("stream-batch-9001")
            lock_observer.allow_batch_silver_body.set()
            batch_future.result(timeout=900)
            cdc_future.result(timeout=900)

        lock_observer.assert_expected_execution()
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
        assert (
            GoldReleaseStore(spark, test_config)
            .snapshot()
            .read_table("gold", "dim_customer")
            .filter(F.col("is_current"))
            .groupBy("source_customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
            == 0
        )
        assert GoldReleaseStore(spark, test_config).latest() is not None
        validate_batch_lakehouse(spark, test_config)
    finally:
        spark.stop()


@dataclass(frozen=True)
class _ObservedLockEvent:
    owner: str
    action: str
    timestamp: float


_LockFactory = Callable[[SparkSession, AppConfig, str], AbstractContextManager[None]]


class _ProductionLockObserver:
    """Coordinate stage contention while delegating every lock operation to Delta."""

    _BATCH_SILVER = "batch-batch-concurrency"
    _BATCH_GOLD = "gold-batch-batch-concurrency"
    _STREAMING_SILVER = "stream-batch-9001"
    _STREAMING_GOLD = "gold-cdc-stream-9001"

    def __init__(self, real_lock: _LockFactory) -> None:
        self._real_lock = real_lock
        self._events: list[_ObservedLockEvent] = []
        self._active_owner: str | None = None
        self._mutex = Lock()
        self.batch_silver_acquired = Event()
        self.streaming_silver_attempted = Event()
        self.allow_batch_silver_body = Event()
        self.streaming_silver_acquired = Event()
        self.streaming_gold_acquired = Event()
        self.batch_gold_attempted = Event()

    @contextmanager
    def lock(self, spark: SparkSession, config: AppConfig, owner: str) -> Iterator[None]:
        if owner == self._STREAMING_SILVER:
            if not self.batch_silver_acquired.wait(timeout=300):
                raise TimeoutError("Batch did not acquire its Silver lock")
            self.streaming_silver_attempted.set()
        elif owner == self._BATCH_GOLD:
            self._record(owner, "stage_ready")
            if not self.streaming_gold_acquired.wait(timeout=300):
                raise TimeoutError("Streaming did not acquire its Gold lock")

        self._record(owner, "attempt")
        if owner == self._BATCH_GOLD:
            self.batch_gold_attempted.set()

        acquired = False
        try:
            with self._real_lock(spark, config, owner):
                self._mark_acquired(owner)
                acquired = True
                if owner == self._BATCH_SILVER:
                    self.batch_silver_acquired.set()
                    if not self.allow_batch_silver_body.wait(timeout=300):
                        raise TimeoutError("Test did not release the Batch Silver stage")
                elif owner == self._STREAMING_SILVER:
                    self.streaming_silver_acquired.set()
                elif owner == self._STREAMING_GOLD:
                    self.streaming_gold_acquired.set()
                    if not self.batch_gold_attempted.wait(timeout=300):
                        raise TimeoutError("Batch did not contend for the Gold lock")
                    sleep(1)
                yield
        finally:
            if acquired:
                self._mark_released(owner)

    def has_acquired(self, owner: str) -> bool:
        with self._mutex:
            return any(event.owner == owner and event.action == "acquire" for event in self._events)

    def assert_expected_execution(self) -> None:
        batch_silver_attempt = self._timestamp(self._BATCH_SILVER, "attempt")
        batch_silver_acquire = self._timestamp(self._BATCH_SILVER, "acquire")
        batch_silver_release = self._timestamp(self._BATCH_SILVER, "release")
        streaming_silver_attempt = self._timestamp(self._STREAMING_SILVER, "attempt")
        streaming_silver_acquire = self._timestamp(self._STREAMING_SILVER, "acquire")
        streaming_silver_release = self._timestamp(self._STREAMING_SILVER, "release")
        batch_gold_ready = self._timestamp(self._BATCH_GOLD, "stage_ready")
        batch_gold_attempt = self._timestamp(self._BATCH_GOLD, "attempt")
        batch_gold_acquire = self._timestamp(self._BATCH_GOLD, "acquire")
        batch_gold_release = self._timestamp(self._BATCH_GOLD, "release")
        streaming_gold_acquire = self._timestamp(self._STREAMING_GOLD, "acquire")
        streaming_gold_release = self._timestamp(self._STREAMING_GOLD, "release")

        assert batch_silver_attempt <= batch_silver_acquire
        assert streaming_silver_attempt < batch_silver_release < streaming_silver_acquire
        assert streaming_silver_acquire - streaming_silver_attempt >= 1
        assert batch_silver_release < batch_gold_ready < streaming_silver_release
        assert streaming_silver_acquire < streaming_gold_acquire < batch_gold_attempt
        assert batch_gold_attempt < streaming_gold_release < batch_gold_acquire
        assert batch_gold_acquire - batch_gold_attempt >= 1
        assert batch_gold_acquire < batch_gold_release

    def _mark_acquired(self, owner: str) -> None:
        with self._mutex:
            assert self._active_owner is None, f"Lock overlap detected: active={self._active_owner}, acquiring={owner}"
            self._active_owner = owner
            self._events.append(_ObservedLockEvent(owner, "acquire", monotonic()))

    def _mark_released(self, owner: str) -> None:
        with self._mutex:
            assert self._active_owner == owner
            self._events.append(_ObservedLockEvent(owner, "release", monotonic()))
            self._active_owner = None

    def _record(self, owner: str, action: str) -> None:
        with self._mutex:
            self._events.append(_ObservedLockEvent(owner, action, monotonic()))

    def _timestamp(self, owner: str, action: str) -> float:
        matching = [event.timestamp for event in self._events if event.owner == owner and event.action == action]
        assert len(matching) == 1, f"Expected one {owner}.{action}, got {matching}"
        return matching[0]


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
