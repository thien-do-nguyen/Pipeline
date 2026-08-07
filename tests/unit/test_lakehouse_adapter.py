import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import Row

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    _delta_sql_identifier,
    _lexicographic_newer_condition,
    _sql_identifier,
    latest_delta_pipeline_commit,
    write_delta,
)
from ecommerce_pipeline.config.models import LakehouseConfig, TableReference


def test_delta_sql_identifier_qualifies_local_relative_path() -> None:
    relative_path = "data/lakehouse/bronze/batch/orders"

    assert _delta_sql_identifier(relative_path) == f"delta.`{(Path.cwd() / relative_path).as_uri()}`"


def test_delta_sql_identifier_preserves_cloud_uri() -> None:
    path = "abfss://lakehouse@example.dfs.core.windows.net/silver/orders"

    assert _delta_sql_identifier(path) == f"delta.`{path}`"


def test_merge_sequence_predicate_is_lexicographic_and_null_safe() -> None:
    condition = _lexicographic_newer_condition(["event_time", "priority", "sequence"])

    assert condition is not None
    assert "source.`event_time` > target.`event_time`" in condition
    assert "source.`event_time` <=> target.`event_time`" in condition
    assert "source.`priority` > target.`priority`" in condition
    assert "source.`sequence` > target.`sequence`" in condition


def test_latest_data_commit_ignores_delta_maintenance_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    latest_history = Mock()
    latest_history.select.return_value.first.return_value = Row(version=9, userMetadata=None)
    fallback_history = Mock()
    fallback_history.select.return_value.collect.return_value = [
        Row(version=9, userMetadata=None),
        Row(version=8, userMetadata=json.dumps({"pipeline": "cdc_to_silver"})),
        Row(version=7, userMetadata=json.dumps({"pipeline": "bronze_to_silver"})),
    ]
    delta = Mock()
    delta.history.side_effect = [latest_history, fallback_history]
    monkeypatch.setattr("ecommerce_pipeline.adapters.lakehouse._delta_table", Mock(return_value=delta))

    commit = latest_delta_pipeline_commit(
        Mock(),
        "data/lakehouse/silver/orders",
        pipelines=("bronze_to_silver", "cdc_to_silver"),
    )

    assert commit.version == 8
    assert commit.metadata["pipeline"] == "cdc_to_silver"


def test_gold_snapshot_reads_the_published_delta_version() -> None:
    spark = Mock()
    reader = spark.read.format.return_value
    reader.option.return_value = reader
    config = Mock()
    config.lakehouse = LakehouseConfig(base_path="data/lakehouse")
    snapshot = LakehouseAdapter(spark, config).with_gold_versions({"fact_sales": 7})

    snapshot.read_table("gold", "fact_sales")

    reader.option.assert_called_once_with("versionAsOf", 7)
    reader.load.assert_called_once_with("data/lakehouse/gold/fact_sales")


def test_gold_snapshot_rejects_unpublished_tables() -> None:
    config = Mock()
    config.lakehouse = LakehouseConfig(base_path="data/lakehouse")
    snapshot = LakehouseAdapter(Mock(), config).with_gold_versions({"fact_sales": 7})

    with pytest.raises(ValueError, match="not part of the published snapshot"):
        snapshot.read_table("gold", "dim_customer")


def test_unity_catalog_reads_and_writes_three_level_table_names() -> None:
    spark = Mock()
    reader = spark.read.format.return_value
    config = Mock()
    config.lakehouse = LakehouseConfig(
        catalog="catalog",
        schemas={"bronze": "bronze", "silver": "silver", "gold": "gold"},
    )

    LakehouseAdapter(spark, config).read_table("silver", "orders")

    reader.table.assert_called_once_with("catalog.silver.orders")
    writer = Mock()
    write_delta(writer, TableReference("catalog.gold.fact_sales", True))
    writer.saveAsTable.assert_called_once_with("catalog.gold.fact_sales")
    assert _sql_identifier(TableReference("catalog.gold.fact_sales", True)) == ("`catalog`.`gold`.`fact_sales`")


def test_unity_catalog_external_table_uses_owned_adls_path() -> None:
    writer = Mock()
    writer.option.return_value = writer
    reference = TableReference(
        "catalog.bronze.orders",
        True,
        "abfss://lakehouse@storage.dfs.core.windows.net/ecommerce/dev/bronze/orders",
    )

    write_delta(writer, reference)

    writer.option.assert_called_once_with("path", reference.storage_path)
    writer.saveAsTable.assert_called_once_with(reference.value)


def test_known_nonempty_source_skips_the_extra_spark_action(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = Mock()
    config = Mock()
    config.lakehouse = LakehouseConfig(base_path="data/lakehouse")
    adapter = LakehouseAdapter(spark, config)
    target = Mock(columns=["id", "value"])
    adapter.read_table = Mock(return_value=target)
    source = Mock(columns=["id", "value"], is_cached=False)
    cached = source.cache.return_value
    merge = Mock()
    delta = Mock()
    delta.alias.return_value.merge.return_value = merge
    merge.whenMatchedUpdate.return_value = merge
    merge.whenNotMatchedInsertAll.return_value = merge
    monkeypatch.setattr("ecommerce_pipeline.adapters.lakehouse._delta_table", Mock(return_value=delta))

    assert adapter.upsert_table(
        source,
        "silver",
        "orders",
        ["id"],
        target_exists=True,
        source_is_nonempty=True,
    )

    cached.isEmpty.assert_not_called()
    merge.execute.assert_called_once_with()
    cached.unpersist.assert_called_once_with()


def test_immutable_dimension_uses_insert_only_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = Mock()
    config = Mock()
    config.lakehouse = LakehouseConfig(base_path="data/lakehouse")
    adapter = LakehouseAdapter(spark, config)
    target = Mock(columns=["key", "value"])
    adapter.read_table = Mock(return_value=target)
    source = Mock(columns=["key", "value"])
    deduplicated = source.dropDuplicates.return_value
    merge = Mock()
    delta = Mock()
    delta.alias.return_value.merge.return_value = merge
    merge.whenNotMatchedInsertAll.return_value = merge
    monkeypatch.setattr("ecommerce_pipeline.adapters.lakehouse._delta_table", Mock(return_value=delta))

    assert adapter.append_new_rows(source, "gold", "dim_date", ["key"], target_exists=True)

    source.dropDuplicates.assert_called_once_with(["key"])
    delta.alias.return_value.merge.assert_called_once_with(
        deduplicated.alias.return_value, "target.`key` = source.`key`"
    )
    merge.whenNotMatchedInsertAll.assert_called_once_with()
    merge.execute.assert_called_once_with()
