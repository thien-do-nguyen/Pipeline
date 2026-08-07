from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES
from ecommerce_pipeline.control.gold_releases import GoldRelease
from ecommerce_pipeline.pipelines import build_gold as gold_module
from ecommerce_pipeline.pipelines.build_gold import GoldBuilder


def _builder() -> GoldBuilder:
    builder = object.__new__(GoldBuilder)
    builder.silver_manifest = None
    builder.timings_ms = None
    builder._paths = Mock(return_value=["gold/fact_sales"])
    builder._run_full = Mock(return_value=frozenset(GOLD_TABLES))
    builder._run_incremental = Mock(return_value=frozenset({"fact_sales"}))
    builder._publish = Mock()
    builder._validate_progress = Mock()
    builder._validate_scd2_schemas = Mock()

    def read_changes(_table: str, _start: int, _end: int) -> Mock:
        changes = Mock()
        changes.is_cached = False
        changes.cache.return_value = changes
        return changes

    builder._read_changes = Mock(side_effect=read_changes)
    builder.releases = Mock()
    return builder


def _release(silver_versions: dict[str, int]) -> GoldRelease:
    return GoldRelease(
        batch_id="previous",
        silver_versions=silver_versions,
        gold_versions={name: index for index, name in enumerate(GOLD_TABLES)},
    )


def test_gold_skips_all_source_reads_when_silver_versions_are_current() -> None:
    builder = _builder()
    builder._silver_versions = Mock(return_value={"orders": 8, "payments": 3})
    builder.releases.latest.return_value = _release({"orders": 8, "payments": 3})

    assert builder.run(batch_id="batch-1") == ["gold/fact_sales"]

    builder._run_full.assert_not_called()
    builder._read_changes.assert_not_called()
    builder._run_incremental.assert_not_called()
    builder._publish.assert_not_called()


def test_gold_reads_only_changed_silver_version_ranges() -> None:
    builder = _builder()
    current = {"orders": 8, "payments": 4}
    builder._silver_versions = Mock(return_value=current)
    builder.releases.latest.return_value = _release({"orders": 8, "payments": 3})

    builder.run(batch_id="batch-2")

    builder._read_changes.assert_called_once_with("payments", 4, 4)
    incremental_changes = builder._run_incremental.call_args.args[0]
    assert set(incremental_changes) == {"payments"}
    incremental_changes["payments"].unpersist.assert_called_once_with()
    builder._publish.assert_called_once_with(
        current,
        "batch-2",
        frozenset({"fact_sales"}),
        builder.releases.latest.return_value,
    )


def test_gold_full_builds_once_when_delta_progress_is_missing() -> None:
    builder = _builder()
    current = {"orders": 8}
    builder._silver_versions = Mock(return_value=current)
    builder.releases.latest.return_value = None

    builder.run(batch_id="batch-3")

    builder._run_full.assert_called_once_with(replace=False)
    builder._run_incremental.assert_not_called()
    builder._publish.assert_called_once_with(
        current,
        "batch-3",
        frozenset(GOLD_TABLES),
        None,
    )


def test_explicit_gold_full_rebuild_replaces_existing_candidates() -> None:
    builder = _builder()
    current = {"orders": 9}
    previous = _release({"orders": 8})
    builder._silver_versions = Mock(return_value=current)
    builder.releases.latest.return_value = previous

    builder.run(batch_id="recovery", full_rebuild=True)

    builder._run_full.assert_called_once_with(replace=True)
    builder._publish.assert_called_once_with(
        current,
        "recovery",
        frozenset(GOLD_TABLES),
        previous,
    )


def test_gold_does_not_publish_when_candidate_write_fails() -> None:
    builder = _builder()
    builder._silver_versions = Mock(return_value={"orders": 8})
    builder.releases.latest.return_value = _release({"orders": 7})
    builder._run_incremental.side_effect = RuntimeError("candidate write failed")

    with pytest.raises(RuntimeError, match="candidate write failed"):
        builder.run(batch_id="batch-failed")

    builder._publish.assert_not_called()


def test_product_changes_do_not_rewrite_historical_facts(spark: SparkSession) -> None:
    builder = object.__new__(GoldBuilder)
    builder.spark = spark
    builder.lakehouse = Mock()
    builder.lakehouse.read_table.return_value = spark.createDataFrame([], "order_id int")
    product_changes = spark.createDataFrame([(101,)], "product_id int")

    affected = builder._affected_order_ids({"products": product_changes})

    assert affected.collect() == []


def test_incremental_immutable_dimension_excludes_unknown_member(spark: SparkSession) -> None:
    builder = object.__new__(GoldBuilder)
    builder.lakehouse = Mock()
    builder.lakehouse.append_new_rows.return_value = True
    dimension = spark.createDataFrame([(0, "unknown"), (101, "known")], "payment_key long, label string")

    assert builder._append_dimension_members(dimension, "dim_payment", "payment_key")

    members = builder.lakehouse.append_new_rows.call_args.args[0]
    assert [(row["payment_key"], row["label"]) for row in members.collect()] == [(101, "known")]
    builder.lakehouse.append_new_rows.assert_called_once_with(
        members,
        "gold",
        "dim_payment",
        ["payment_key"],
        target_exists=True,
    )


def test_gold_filter_can_map_different_id_column_names(spark: SparkSession) -> None:
    builder = object.__new__(GoldBuilder)
    builder.spark = spark
    builder.lakehouse = Mock()
    categories = spark.createDataFrame(
        [(1, None), (2, 1), (3, 2)],
        "category_id int, parent_category_id int",
    )
    builder.lakehouse.read_table.return_value = categories
    changed_category_ids = spark.createDataFrame([(1,)], "category_id int")

    children = builder._filter_current(
        "categories",
        "parent_category_id",
        changed_category_ids,
        "category_id",
    )

    assert [row["category_id"] for row in children.collect()] == [2]


def test_incremental_checkpoints_reused_affected_order_ids() -> None:
    builder = object.__new__(GoldBuilder)
    changed_order_ids = Mock()
    affected_plan = Mock()
    affected_ids = Mock()
    affected_plan.localCheckpoint.return_value = affected_ids
    affected_ids.isEmpty.return_value = False
    builder._ids_if_changed = Mock(return_value=changed_order_ids)
    builder._affected_order_ids = Mock(return_value=affected_plan)
    changed_gold_tables = frozenset({"dim_date", "fact_sales"})
    builder._apply_incremental = Mock(return_value=changed_gold_tables)
    changes = {"orders": Mock()}

    assert builder._run_incremental(changes) == changed_gold_tables

    affected_plan.localCheckpoint.assert_called_once_with(eager=True)
    builder._apply_incremental.assert_called_once_with(
        changes,
        changed_order_ids,
        affected_ids,
        has_affected_orders=True,
    )
    affected_ids.unpersist.assert_called_once_with()


def test_publisher_reads_versions_only_for_changed_gold_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = object.__new__(GoldBuilder)
    builder.spark = Mock()
    builder.config = SimpleNamespace(
        lakehouse=SimpleNamespace(
            table_reference=lambda layer, table: f"{layer}.{table}",
        )
    )
    builder.releases = Mock()
    previous = _release({"orders": 4})
    version_reader = Mock(return_value=99)
    monkeypatch.setattr(gold_module, "latest_delta_version", version_reader)

    builder._publish(
        {"orders": 5},
        "batch-5",
        frozenset({"dim_payment"}),
        previous,
    )

    version_reader.assert_called_once_with(builder.spark, "gold.dim_payment")
    candidate = builder.releases.publish.call_args.args[0]
    assert candidate.previous_versions == previous.gold_versions
    assert candidate.committed_versions["dim_payment"] == 99
    assert candidate.committed_versions["dim_customer"] == previous.gold_versions["dim_customer"]
    assert candidate.quality_status == "PASSED"
