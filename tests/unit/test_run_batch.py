from __future__ import annotations

from argparse import Namespace
from contextlib import nullcontext
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


def test_record_count_is_absent_when_process_did_not_run_bronze() -> None:
    assert run_batch._record_count([]) is None


def test_record_count_preserves_a_real_zero_event_bronze_run() -> None:
    result = BronzeTableResult(
        batch_id="batch",
        table_name="orders",
        record_count=0,
        ingestion_type="incremental",
        delta_version=7,
    )

    assert run_batch._record_count([result]) == 0


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
        record_count=0,
        ingestion_type="incremental",
        delta_version=7,
    )
    bronze_manifest = BronzeBatchManifest.from_results("batch", [result])
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    silver = Mock()
    gold = Mock()
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "log_batch_run_status", Mock())
    downstream_is_current = Mock(return_value=True)
    monkeypatch.setattr(run_batch, "_downstream_is_current", downstream_is_current)
    args = Namespace(
        mode="all",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(
        application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"),
    )

    results = run_batch.run_mode(Mock(), args, config, "batch", {})

    assert isinstance(results, BronzeBatchManifest)
    assert results.results == [result]
    silver.assert_not_called()
    gold.assert_not_called()
    downstream_is_current.assert_called_once()


def test_all_mode_bootstraps_missing_downstream_after_unchanged_bronze(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = BronzeTableResult(
        batch_id="batch",
        table_name="orders",
        record_count=0,
        ingestion_type="incremental",
        delta_version=7,
    )
    bronze_manifest = BronzeBatchManifest.from_results("batch", [result])
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    monkeypatch.setattr(run_batch, "_downstream_is_current", Mock(return_value=False))
    silver_manifest = Mock(tables={})
    silver = Mock(return_value=silver_manifest)
    gold = Mock(return_value=[])
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "log_batch_run_status", Mock())
    args = Namespace(
        mode="all",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(
        application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"),
    )

    run_batch.run_mode(Mock(), args, config, "batch", {})

    silver.assert_called_once()
    gold.assert_called_once()


def test_validate_mode_runs_quality_without_writing_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    report = Mock()
    report.as_dict.return_value = {"gold": {"status": "passed"}}
    validate = Mock(return_value=report)
    monkeypatch.setattr(run_batch, "validate_batch_lakehouse", validate)
    args = Namespace(
        mode="validate",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    timings: dict[str, int] = {}
    spark = Mock()
    config = SimpleNamespace()

    results = run_batch.run_mode(spark, args, config, "batch", timings)

    assert results is None
    assert "validation" in timings
    validate.assert_called_once_with(spark, config)


def test_validate_release_uses_propagated_silver_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = SilverBatchManifest(
        tables={"orders": SilverTableResult("orders", committed_version=5, schema_version=2)}
    )
    validate = Mock(return_value={"batch_id": "gold-1"})
    monkeypatch.setattr(run_batch, "validate_gold_release", validate)
    args = Namespace(
        mode="validate_release",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    timings: dict[str, int] = {}
    spark = Mock()
    config = SimpleNamespace()

    result = run_batch.run_mode(spark, args, config, "batch", timings, upstream_manifest=manifest)

    assert result is None
    assert "validation.release" in timings
    validate.assert_called_once_with(spark, config, manifest)


def test_validate_release_uses_control_state_without_xcom_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validate = Mock(return_value={"batch_id": "gold-1"})
    monkeypatch.setattr(run_batch, "validate_gold_release", validate)
    args = Namespace(
        mode="validate_release",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"))
    spark = Mock()

    run_batch.run_mode(spark, args, config, "batch", {})

    validate.assert_called_once_with(spark, config, None)


def test_preflight_mode_does_not_write_lakehouse_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    check = Mock(return_value={"status": "passed"})
    monkeypatch.setattr(run_batch, "check_postgres_source", check)
    args = Namespace(
        mode="check_postgres",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    timings: dict[str, int] = {}
    spark = Mock()
    config = SimpleNamespace()

    results = run_batch.run_mode(spark, args, config, "batch", timings)

    assert results is None
    assert "preflight.postgres" in timings
    check.assert_called_once_with(config)


def test_cloud_gold_build_is_serialized_by_shared_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    gold = Mock(return_value=["catalog.gold.fact_sales"])
    lock = Mock(return_value=nullcontext())
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "cloud_pipeline_lock", lock)
    monkeypatch.setattr(run_batch, "log_batch_run_status", Mock())
    args = Namespace(
        mode="gold",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    spark = Mock()
    config = SimpleNamespace(
        application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"),
        spark=SimpleNamespace(master=None),
    )

    run_batch.run_mode(spark, args, config, "batch-42", {})

    lock.assert_called_once_with(spark, config, "gold-batch-batch-42")
    gold.assert_called_once()


def test_all_mode_builds_gold_after_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    bronze = BronzeTableResult(
        batch_id="batch",
        table_name="orders",
        record_count=2,
        ingestion_type="incremental",
        delta_version=8,
        operation_counts={"INSERT": 2},
    )
    bronze_manifest = BronzeBatchManifest.from_results("batch", [bronze])
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    silver_manifest = SilverBatchManifest(
        tables={
            "orders": SilverTableResult(
                table_name="orders",
                committed_version=5,
                schema_version=1,
            )
        },
    )
    silver = Mock(return_value=silver_manifest)
    gold = Mock(return_value=["catalog.gold.fact_sales"])
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "log_batch_run_status", Mock())
    args = Namespace(
        mode="all",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(
        application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"),
    )

    run_batch.run_mode(Mock(), args, config, "batch", {})

    assert silver.call_args.kwargs["bronze_manifest"] is bronze_manifest
    assert gold.call_args.kwargs["silver_manifest"] is silver_manifest


def test_workflow_mode_runs_preflight_layers_and_release_validation_in_one_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bronze_manifest = BronzeBatchManifest.from_results(
        "batch",
        [BronzeTableResult("batch", "orders", 2, "incremental", delta_version=8)],
    )
    silver_manifest = SilverBatchManifest(
        tables={"orders": SilverTableResult("orders", committed_version=5, schema_version=2)}
    )
    preflight = Mock(return_value={"status": "passed"})
    silver = Mock(return_value=silver_manifest)
    gold = Mock(return_value=["catalog.gold.fact_sales"])
    validate = Mock(return_value={"status": "passed"})
    monkeypatch.setattr(run_batch, "check_postgres_source", preflight)
    monkeypatch.setattr(run_batch, "extract_all_to_bronze", Mock(return_value=bronze_manifest))
    monkeypatch.setattr(run_batch, "build_silver", silver)
    monkeypatch.setattr(run_batch, "build_gold", gold)
    monkeypatch.setattr(run_batch, "validate_gold_release", validate)
    monkeypatch.setattr(run_batch, "log_batch_run_status", Mock())
    args = Namespace(
        mode="workflow",
        tables=None,
        full_rebuild_silver=False,
        full_rebuild_gold=False,
    )
    config = SimpleNamespace(application=SimpleNamespace(timezone="Asia/Ho_Chi_Minh"))
    spark = Mock()

    result = run_batch.run_mode(spark, args, config, "batch", {})

    assert result is bronze_manifest
    preflight.assert_called_once_with(config)
    assert silver.call_args.kwargs["bronze_manifest"] is bronze_manifest
    assert gold.call_args.kwargs["silver_manifest"] is silver_manifest
    validate.assert_called_once_with(spark, config, silver_manifest)
