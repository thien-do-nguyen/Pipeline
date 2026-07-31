from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.control.manifests import (
    BronzeBatchManifest,
    BronzeTableResult,
    SilverBatchManifest,
    SilverTableResult,
)
from ecommerce_pipeline.jobs import run_batch


def test_prepare_databricks_environment_sets_non_secret_values(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "DATABRICKS_CATALOG",
        "DATABRICKS_BRONZE_SCHEMA",
        "DATABRICKS_SILVER_SCHEMA",
        "DATABRICKS_GOLD_SCHEMA",
        "DATABRICKS_EXTERNAL_STORAGE_ROOT",
    ):
        monkeypatch.delenv(name, raising=False)
    args = Namespace(
        secret_scope="ecommerce-pipeline",
        postgres_host="postgres.example",
        postgres_port="5432",
        postgres_database="postgres",
        postgres_user="pipeline",
        uc_catalog="catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        external_storage_root="abfss://lakehouse@storage.dfs.core.windows.net/pipeline/dev",
    )

    run_batch._prepare_databricks_environment(args)

    assert run_batch.os.environ["POSTGRES_HOST"] == "postgres.example"
    assert run_batch.os.environ["POSTGRES_PASSWORD"] == "__DATABRICKS_SECRET__"
    assert run_batch.os.environ["DATABRICKS_CATALOG"] == "catalog"


def test_apply_databricks_secret_updates_only_postgres_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "POSTGRES_HOST": "postgres.example",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "postgres",
        "POSTGRES_USER": "pipeline",
        "POSTGRES_PASSWORD": "__DATABRICKS_SECRET__",
        "DATABRICKS_CATALOG": "catalog",
        "DATABRICKS_BRONZE_SCHEMA": "bronze",
        "DATABRICKS_SILVER_SCHEMA": "silver",
        "DATABRICKS_GOLD_SCHEMA": "gold",
        "DATABRICKS_EXTERNAL_STORAGE_ROOT": "abfss://lakehouse@storage.dfs.core.windows.net/pipeline/dev",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    config = load_config("azure", dotenv_path=Path("/tmp/missing.env"))

    class FakeSecrets:
        def get(self, scope: str, key: str) -> str:
            assert scope == "ecommerce-pipeline"
            assert key == "postgres-password"
            return "postgres-secret"

    class FakeDBUtils:
        secrets = FakeSecrets()

    class FakeSpark:
        pass

    spark = FakeSpark()
    monkeypatch.setattr(run_batch, "_databricks_utils", lambda _: FakeDBUtils())

    updated = run_batch._apply_databricks_postgres_secret(
        cast(SparkSession, spark),
        config,
        "ecommerce-pipeline",
    )

    assert updated.postgres.password_value == "postgres-secret"
    assert config.postgres.password_value == "__DATABRICKS_SECRET__"


def test_all_mode_stops_after_unchanged_bronze(monkeypatch: pytest.MonkeyPatch) -> None:
    result = BronzeTableResult(
        batch_id="batch",
        table_name="orders",
        output_path="catalog.bronze.orders",
        record_count=0,
        ingestion_type="incremental",
        delta_version=7,
        previous_delta_version=7,
    )
    bronze_manifest = BronzeBatchManifest.from_results("batch", [result])
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    silver = Mock()
    gold = Mock()
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "write_batch_run_status", Mock())
    args = Namespace(
        mode="all",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs", timezone="Asia/Ho_Chi_Minh"),
    )

    results, outputs = run_batch.run_mode(Mock(), args, config, "batch", {})

    assert results == [result]
    assert outputs == {"bronze": ["catalog.bronze.orders"]}
    silver.assert_not_called()
    gold.assert_not_called()


def test_all_mode_propagates_layer_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    bronze = BronzeTableResult(
        batch_id="batch",
        table_name="orders",
        output_path="catalog.bronze.orders",
        record_count=2,
        ingestion_type="incremental",
        delta_version=8,
        previous_delta_version=7,
        operation_counts={"INSERT": 2},
    )
    bronze_manifest = BronzeBatchManifest.from_results("batch", [bronze])
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    silver_manifest = SilverBatchManifest(
        batch_id="batch",
        tables={
            "orders": SilverTableResult(
                table_name="orders",
                output_path="catalog.silver.orders",
                processing_mode="cdf",
                bronze_starting_version=8,
                bronze_ending_version=8,
                committed_version=5,
                schema_version=1,
                source_record_count=2,
                source_operation_counts={"INSERT": 2},
            )
        },
    )
    silver = Mock(return_value=silver_manifest)
    gold = Mock(return_value=["catalog.gold.fact_sales"])
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "write_batch_run_status", Mock())
    args = Namespace(
        mode="all",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(
        application=SimpleNamespace(logs_path="logs", timezone="Asia/Ho_Chi_Minh"),
    )

    run_batch.run_mode(Mock(), args, config, "batch", {})

    assert silver.call_args.kwargs["bronze_manifest"] is bronze_manifest
    assert gold.call_args.kwargs["silver_manifest"] is silver_manifest
