from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from pyspark.sql import Row

from ecommerce_pipeline.ingestion.streaming import unified_silver
from ecommerce_pipeline.ingestion.streaming.unified_silver import UnifiedSilverMaterializer
from ecommerce_pipeline.pipelines.quality import GoldQualityError
from ecommerce_pipeline.transformations.silver.common import SILVER_SEQUENCE_COLUMNS


def test_foreach_batch_reads_materialized_typed_bronze_and_stops_at_shared_silver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.table_names = ("orders",)
    materializer._targets_validated = False
    materializer._transform_table = Mock(return_value="silver-orders")
    materializer._transform_history_table = Mock(return_value="history-orders")
    materializer._preflight_targets = Mock(return_value={"orders": True})
    materializer._merge_table = Mock()
    materializer.silver = Mock()
    materializer.spark = Mock()
    materializer.settings = SimpleNamespace(query_name="cdc-to-silver", checkpoint_version="v1")
    materializer.config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs"),
        spark=SimpleNamespace(master="local[2]", max_parallel_tables=1),
        coordination=SimpleNamespace(gold_owner="streaming", lock_wait_seconds=0),
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
    materializer._transform_history_table.assert_called_once_with("typed-orders", "orders")
    materializer.silver.append_change_history.assert_called_once()
    gold.run.assert_not_called()


def test_foreach_batch_reconciles_gold_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.table_names = ("orders",)
    materializer._targets_validated = False
    materializer._transform_table = Mock(return_value="silver-orders")
    materializer._transform_history_table = Mock(return_value="history-orders")
    materializer._preflight_targets = Mock(return_value={"orders": True})
    materializer._merge_table = Mock()
    materializer.silver = Mock()
    materializer._track_pending_gold_scope = Mock()
    materializer.reconcile_gold = Mock(return_value="published")
    materializer._pending_gold_order_ids = {42}
    materializer._pending_gold_requires_full_readiness = False
    materializer.spark = Mock()
    materializer.settings = SimpleNamespace(
        query_name="cdc-to-silver",
        checkpoint_version="v1",
        reconcile_gold_each_batch=True,
    )
    materializer.config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs"),
        spark=SimpleNamespace(master="local[2]", max_parallel_tables=1),
        coordination=SimpleNamespace(gold_owner="streaming", lock_wait_seconds=0),
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

    materializer.reconcile_gold.assert_called_once_with(
        batch_id="cdc-stream-11",
        raise_on_quality_error=False,
        rebuild_on_quality_error=False,
        defer_if_source_incomplete=True,
        affected_order_ids={42},
    )


def test_gold_reconcile_defers_when_source_fact_is_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs"),
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            gold_owner="streaming",
            lock_wait_seconds=0,
        ),
    )
    materializer._source_fact_ready_for_gold = Mock(return_value=False)
    gold_builder = Mock()
    monkeypatch.setattr(unified_silver, "GoldBuilder", gold_builder)

    status = materializer.reconcile_gold(defer_if_source_incomplete=True)

    assert status == "deferred_source_incomplete"
    gold_builder.assert_not_called()


def test_per_batch_gold_reconcile_retries_deferred_source(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.settings = SimpleNamespace(
        gold_deferred_retry_seconds=10,
        gold_deferred_retry_interval_seconds=2,
    )
    materializer.reconcile_gold = Mock(side_effect=["deferred_source_incomplete", "published"])
    sleep_mock = Mock()
    monkeypatch.setattr(unified_silver, "sleep", sleep_mock)

    status = materializer._reconcile_gold_with_deferred_retry(
        batch_id="cdc-stream-9",
        raise_on_quality_error=False,
        rebuild_on_quality_error=False,
        defer_if_source_incomplete=True,
        affected_order_ids={101},
    )

    assert status == "published"
    sleep_mock.assert_called_once_with(2)
    assert materializer.reconcile_gold.call_args_list == [
        call(
            batch_id="cdc-stream-9",
            raise_on_quality_error=False,
            rebuild_on_quality_error=False,
            defer_if_source_incomplete=True,
            affected_order_ids={101},
        ),
        call(
            batch_id="cdc-stream-9-retry-1",
            raise_on_quality_error=False,
            rebuild_on_quality_error=False,
            defer_if_source_incomplete=True,
            affected_order_ids={101},
        ),
    ]


def test_idle_gold_reconcile_publishes_pending_scope() -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.settings = SimpleNamespace(reconcile_gold_each_batch=True)
    materializer._pending_gold_order_ids = {935}
    materializer._pending_gold_requires_full_readiness = False
    materializer.reconcile_gold = Mock(return_value="published")

    status = materializer.reconcile_pending_gold_if_ready(batch_id="cdc-idle-reconcile")

    assert status == "published"
    assert materializer._pending_gold_order_ids == set()
    assert materializer._pending_gold_requires_full_readiness is False
    materializer.reconcile_gold.assert_called_once_with(
        batch_id="cdc-idle-reconcile",
        raise_on_quality_error=False,
        rebuild_on_quality_error=False,
        defer_if_source_incomplete=True,
        affected_order_ids={935},
    )


def test_per_batch_gold_reconcile_skips_failed_publish_without_rebuild(monkeypatch: pytest.MonkeyPatch) -> None:
    materializer = object.__new__(UnifiedSilverMaterializer)
    materializer.spark = Mock()
    materializer.config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs"),
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            gold_owner="streaming",
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
        application=SimpleNamespace(logs_path="logs"),
        spark=SimpleNamespace(master="local[2]"),
        coordination=SimpleNamespace(
            gold_owner="streaming",
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
        "logs",
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
    )
    transaction.assert_called_once_with(
        worker_spark,
        application_id="cdc-to-silver-v1-orders",
        transaction_version=7,
    )
