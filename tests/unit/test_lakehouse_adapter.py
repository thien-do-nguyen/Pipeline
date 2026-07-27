from pathlib import Path
from unittest.mock import Mock

import pytest

from ecommerce_pipeline.adapters.lakehouse import LakehouseAdapter, _delta_sql_identifier
from ecommerce_pipeline.config.models import LakehouseConfig


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
