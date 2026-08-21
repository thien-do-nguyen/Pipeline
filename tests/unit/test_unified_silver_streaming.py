from contextlib import nullcontext
from decimal import Decimal
from threading import Lock
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from pyspark.sql import Row, SparkSession

from ecommerce_pipeline.control.gold_reconcile_queue import PendingGoldScope
from ecommerce_pipeline.ingestion.streaming import unified_silver
from ecommerce_pipeline.ingestion.streaming.unified_silver import UnifiedSilverMaterializer
from ecommerce_pipeline.pipelines.quality import GoldQualityError
from ecommerce_pipeline.transformations.silver.common import SILVER_SEQUENCE_COLUMNS


def test_foreach_batch_reads_materialized_typed_bronze_and_stops_at_shared_silver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer._cycle_lock = Lock()
    materializer.table_names = ("orders",)
    materializer._targets_validated = False
    materializer._transform_table = Mock(return_value="silver-orders")
    materializer._transform_history_table = Mock(return_value="history-orders")
    materializer._preflight_targets = Mock(return_value={"orders": True})
    materializer._merge_table = Mock()
    materializer.silver = Mock()
    materializer.gold_queue = Mock()
    materializer._gold_scope = Mock(return_value=(set(), False))
    materializer.spark = Mock()
    materializer.settings = SimpleNamespace(query_name="cdc-to-silver", checkpoint_version="v1")
    materializer.config = SimpleNamespace(
        spark=SimpleNamespace(master="local[2]", max_parallel_tables=1),
        coordination=SimpleNamespace(local_lock_path="data/runtime", lock_wait_seconds=0),
    )
    prepared = Mock()
    prepared.persist.return_value = prepared
    statistics = {
        "orders": Row(record_count=3, min_source_lsn=100, max_source_lsn=200),
    }
    monkeypatch.setattr(unified_silver, "prepare_raw_cdc_events", Mock(return_value=prepared))
    materializer.typed_bronze = Mock()
    materializer.typed_bronze.materialize.return_value = SimpleNamespace(
        tables={"orders": "typed-orders"},
        statistics=statistics,
        quarantined_count=1,
    )
    monkeypatch.setattr(unified_silver, "local_pipeline_lock", Mock(return_value=nullcontext()))
    gold = Mock()
    monkeypatch.setattr(unified_silver, "GoldBuilder", Mock(return_value=gold))

    materializer.process_batch(Mock(), 11)

    materializer._merge_table.assert_called_once_with(
        "silver-orders",
        "orders",
        11,
        statistics["orders"],
        target_exists=True,
    )
    materializer._transform_table.assert_called_once_with("typed-orders", "orders")
    materializer._transform_history_table.assert_not_called()
    materializer.silver.append_change_history.assert_not_called()
    materializer.gold_queue.enqueue.assert_called_once_with(
        request_id="cdc-to-silver:v1:11",
        affected_order_ids=set(),
        requires_fact_readiness=False,
    )
    gold.run.assert_not_called()


def test_foreach_batch_queues_gold_without_running_it_inline(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer._cycle_lock = Lock()
    materializer.table_names = ("orders",)
    materializer._targets_validated = False
    materializer._transform_table = Mock(return_value="silver-orders")
    materializer._transform_history_table = Mock(return_value="history-orders")
    materializer._preflight_targets = Mock(return_value={"orders": True})
    materializer._merge_table = Mock()
    materializer.silver = Mock()
    materializer.gold_queue = Mock()
    materializer._gold_scope = Mock(return_value=({42}, True))
    materializer.spark = Mock()
    materializer.settings = SimpleNamespace(
        query_name="cdc-to-silver",
        checkpoint_version="v1",
    )
    materializer.config = SimpleNamespace(
        spark=SimpleNamespace(master="local[2]", max_parallel_tables=1),
        coordination=SimpleNamespace(local_lock_path="data/runtime", lock_wait_seconds=0),
    )
    prepared = Mock()
    prepared.persist.return_value = prepared
    statistics = {
        "orders": Row(record_count=3, min_source_lsn=100, max_source_lsn=200),
    }
    monkeypatch.setattr(unified_silver, "prepare_raw_cdc_events", Mock(return_value=prepared))
    materializer.typed_bronze = Mock()
    materializer.typed_bronze.materialize.return_value = SimpleNamespace(
        tables={"orders": "typed-orders"},
        statistics=statistics,
        quarantined_count=0,
    )
    monkeypatch.setattr(unified_silver, "local_pipeline_lock", Mock(return_value=nullcontext()))

    materializer.process_batch(Mock(), 11)

    materializer.gold_queue.enqueue.assert_called_once_with(
        request_id="cdc-to-silver:v1:11",
        affected_order_ids={42},
        requires_fact_readiness=True,
    )


def test_gold_reconcile_defers_when_source_fact_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = SimpleNamespace(
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            local_lock_path="data/runtime",
            lock_wait_seconds=0,
        ),
    )
    materializer._source_fact_ready_for_gold = Mock(return_value=False)
    gold_builder = Mock()
    monkeypatch.setattr(unified_silver, "GoldBuilder", gold_builder)

    status = materializer.reconcile_gold(defer_if_source_incomplete=True)

    assert status == "deferred_source_incomplete"
    gold_builder.assert_not_called()


def test_delete_readiness_completes_after_order_and_items_are_tombstoned(spark: SparkSession) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = spark
    materializer.lakehouse = Mock()
    orders = spark.createDataFrame(
        [(42, True, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, subtotal_amount decimal(18,2), tax_amount decimal(18,2)",
    )
    items = spark.createDataFrame(
        [(42, True, 1, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2)",
    )
    materializer.lakehouse.read_table.side_effect = lambda _layer, table_name, **_kwargs: (
        orders if table_name == "orders" else items
    )

    assert materializer._source_fact_ready_for_gold({42}) is True
    assert materializer.lakehouse.read_table.call_args_list == [
        call("silver", "orders", include_deleted=True),
        call("silver", "order_items", include_deleted=True),
    ]


def test_delete_readiness_waits_when_order_tombstone_arrives_before_item_delete(spark: SparkSession) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = spark
    materializer.lakehouse = Mock()
    orders = spark.createDataFrame(
        [(42, True, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, subtotal_amount decimal(18,2), tax_amount decimal(18,2)",
    )
    active_items = spark.createDataFrame(
        [(42, False, 1, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2)",
    )
    materializer.lakehouse.read_table.side_effect = lambda _layer, table_name, **_kwargs: (
        orders if table_name == "orders" else active_items
    )

    assert materializer._source_fact_ready_for_gold({42}) is False


def test_delete_readiness_waits_when_item_delete_arrives_before_order_delete(spark: SparkSession) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = spark
    materializer.lakehouse = Mock()
    active_order = spark.createDataFrame(
        [(42, False, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, subtotal_amount decimal(18,2), tax_amount decimal(18,2)",
    )
    deleted_items = spark.createDataFrame(
        [(42, True, 1, Decimal("100.00"), Decimal("8.00"))],
        "order_id long, _is_deleted boolean, quantity int, unit_price decimal(18,2), tax_amount decimal(18,2)",
    )
    materializer.lakehouse.read_table.side_effect = lambda _layer, table_name, **_kwargs: (
        active_order if table_name == "orders" else deleted_items
    )

    assert materializer._source_fact_ready_for_gold({42}) is False


def test_idle_gold_reconcile_publishes_pending_scope() -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer._cycle_lock = Lock()
    materializer.settings = SimpleNamespace(max_gold_readiness_order_ids=5000)
    pending = PendingGoldScope(
        keys=(("request-1", "order:935"),),
        affected_order_ids={935},
        requires_fact_readiness=True,
    )
    materializer.gold_queue = Mock()
    materializer.gold_queue.pending.side_effect = [pending, pending]
    materializer._writer_lock = Mock(return_value=nullcontext())
    materializer._source_fact_ready_for_gold = Mock(return_value=True)
    materializer._run_gold_locked = Mock(return_value="published")

    status = materializer.reconcile_pending_gold_if_ready(batch_id="cdc-idle-reconcile")

    assert status == "published"
    materializer._run_gold_locked.assert_called_once_with(
        batch_id="cdc-idle-reconcile",
        raise_on_quality_error=False,
        rebuild_on_quality_error=False,
    )
    materializer.gold_queue.complete.assert_called_once_with(pending.keys)


def test_idle_gold_reconcile_does_not_lock_when_queue_is_empty() -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer._cycle_lock = Lock()
    materializer.settings = SimpleNamespace(max_gold_readiness_order_ids=5000)
    materializer.gold_queue = Mock()
    materializer.gold_queue.pending.return_value = None
    materializer._writer_lock = Mock()

    assert materializer.reconcile_pending_gold_if_ready(batch_id="cdc-idle-reconcile") == "not_pending"
    materializer._writer_lock.assert_not_called()


def test_per_batch_gold_reconcile_skips_failed_publish_without_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = SimpleNamespace(
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            local_lock_path="data/runtime",
            lock_wait_seconds=0,
        ),
    )
    materializer._source_fact_ready_for_gold = Mock(return_value=True)
    gold = Mock()
    gold.run.side_effect = GoldQualityError("partial source")
    monkeypatch.setattr(unified_silver, "GoldBuilder", Mock(return_value=gold))
    monkeypatch.setattr(unified_silver, "local_pipeline_lock", Mock(return_value=nullcontext()))

    status = materializer.reconcile_gold(
        raise_on_quality_error=False,
        rebuild_on_quality_error=False,
        defer_if_source_incomplete=True,
    )

    assert status == "quality_skipped"
    assert gold.run.call_count == 1


def test_final_gold_reconcile_recovers_a_dirty_candidate_with_full_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = SimpleNamespace(
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            local_lock_path="data/runtime",
            lock_wait_seconds=0,
        ),
    )
    gold = Mock()
    gold.run.side_effect = [GoldQualityError("dirty candidate"), []]
    monkeypatch.setattr(unified_silver, "GoldBuilder", Mock(return_value=gold))
    pipeline_lock = Mock(return_value=nullcontext())
    monkeypatch.setattr(unified_silver, "local_pipeline_lock", pipeline_lock)

    materializer.reconcile_gold()

    assert gold.run.call_args_list[0].kwargs == {"batch_id": "cdc-final-reconcile"}
    assert gold.run.call_args_list[1].kwargs == {
        "batch_id": "cdc-final-reconcile-rebuild",
        "full_rebuild": True,
    }
    pipeline_lock.assert_called_once_with(
        "data/runtime",
        "gold-cdc-final-reconcile",
        wait_timeout_seconds=0,
    )


def test_cdc_merge_writes_the_shared_silver_target_with_order_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    worker_spark = materializer.spark.newSession.return_value
    materializer.config = Mock()
    materializer.settings = SimpleNamespace(query_name="cdc-to-silver", checkpoint_version="v1")
    lakehouse = Mock()
    dataframe = Mock()
    transaction = Mock()
    metadata = Mock()
    monkeypatch.setattr(unified_silver, "delta_idempotent_transaction", transaction)
    monkeypatch.setattr(unified_silver, "delta_commit_metadata", metadata)
    monkeypatch.setattr(unified_silver, "LakehouseAdapter", Mock(return_value=lakehouse))
    transaction.return_value = nullcontext()
    metadata.return_value = nullcontext()

    materializer._merge_table(
        dataframe,
        "orders",
        7,
        Row(record_count=2, min_source_lsn=10, max_source_lsn=20),
        target_exists=True,
    )

    lakehouse.upsert_table.assert_called_once_with(
        dataframe,
        "silver",
        "orders",
        ("order_id",),
        delete_mode="soft",
        sequence_columns=SILVER_SEQUENCE_COLUMNS,
        target_exists=True,
        source_is_nonempty=True,
        preserve_target_on_soft_delete=True,
    )
    transaction.assert_called_once_with(
        worker_spark,
        application_id="cdc-to-silver-v1-orders",
        transaction_version=7,
    )


def test_cdc_bootstrap_uses_dataframe_owner_session_for_commit_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = Mock()
    materializer.settings = SimpleNamespace(query_name="cdc-to-silver", checkpoint_version="v1")
    lakehouse = Mock()
    dataframe = Mock()
    transaction = Mock(return_value=nullcontext())
    metadata = Mock(return_value=nullcontext())
    monkeypatch.setattr(unified_silver, "delta_idempotent_transaction", transaction)
    monkeypatch.setattr(unified_silver, "delta_commit_metadata", metadata)
    monkeypatch.setattr(unified_silver, "LakehouseAdapter", Mock(return_value=lakehouse))

    materializer._merge_table(
        dataframe,
        "orders",
        7,
        Row(record_count=2, min_source_lsn=10, max_source_lsn=20),
        target_exists=False,
    )

    materializer.spark.newSession.assert_not_called()
    transaction.assert_called_once_with(
        materializer.spark,
        application_id="cdc-to-silver-v1-orders",
        transaction_version=7,
    )
    metadata.assert_called_once()
    lakehouse.write_table.assert_called_once_with(
        dataframe,
        "silver",
        "orders",
        enable_change_data_feed=True,
    )
