from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from ecommerce_pipeline.control.gold_releases import GoldRelease
from ecommerce_pipeline.pipelines.build_gold import GoldBuilder


def _builder() -> GoldBuilder:
    builder = object.__new__(GoldBuilder)
    builder._paths = Mock(return_value=["gold/fact_sales"])
    builder._run_full = Mock()
    builder._run_incremental = Mock()
    builder._publish = Mock()
    builder._validate_progress = Mock()
    builder._validate_scd2_schemas = Mock()
    builder._read_changes = Mock(side_effect=lambda table, start, end: (table, start, end))
    builder.releases = Mock()
    return builder


def _release(silver_versions: dict[str, int]) -> GoldRelease:
    return GoldRelease(batch_id="previous", silver_versions=silver_versions, gold_versions={})


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
    builder._run_incremental.assert_called_once_with({"payments": ("payments", 4, 4)})
    builder._publish.assert_called_once_with(current, "batch-2")


def test_gold_full_builds_once_when_delta_progress_is_missing() -> None:
    builder = _builder()
    current = {"orders": 8}
    builder._silver_versions = Mock(return_value=current)
    builder.releases.latest.return_value = None

    builder.run(batch_id="batch-3")

    builder._run_full.assert_called_once_with()
    builder._run_incremental.assert_not_called()
    builder._publish.assert_called_once_with(current, "batch-3")


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
