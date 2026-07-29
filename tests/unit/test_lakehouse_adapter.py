from pathlib import Path
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    _delta_sql_identifier,
    _sql_identifier,
    write_delta,
)
from ecommerce_pipeline.config.models import LakehouseConfig, TableReference


def test_delta_sql_identifier_qualifies_local_relative_path() -> None:
    relative_path = "data/lakehouse/bronze/batch/orders"

    assert _delta_sql_identifier(relative_path) == f"delta.`{(Path.cwd() / relative_path).as_uri()}`"


def test_delta_sql_identifier_preserves_cloud_uri() -> None:
    path = "abfss://lakehouse@example.dfs.core.windows.net/silver/orders"

    assert _delta_sql_identifier(path) == f"delta.`{path}`"


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
