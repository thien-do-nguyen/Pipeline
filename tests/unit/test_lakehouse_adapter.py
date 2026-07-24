from pathlib import Path

from ecommerce_pipeline.adapters.lakehouse import _delta_sql_identifier


def test_delta_sql_identifier_qualifies_local_relative_path() -> None:
    relative_path = "data/lakehouse/bronze/batch/orders"

    assert _delta_sql_identifier(relative_path) == f"delta.`{(Path.cwd() / relative_path).as_uri()}`"


def test_delta_sql_identifier_preserves_cloud_uri() -> None:
    path = "abfss://lakehouse@example.dfs.core.windows.net/silver/orders"

    assert _delta_sql_identifier(path) == f"delta.`{path}`"
