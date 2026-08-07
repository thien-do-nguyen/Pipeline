from __future__ import annotations

from pathlib import Path

import pytest

from ecommerce_pipeline.config import loader
from ecommerce_pipeline.config.loader import ConfigError, load_config
from ecommerce_pipeline.config.models import LakehouseConfig

BASE_YAML = """
application:
  name: pipeline
  source_system: ecommerce
  timezone: Asia/Ho_Chi_Minh
postgres:
  host: "${POSTGRES_HOST}"
  port: "${POSTGRES_PORT}"
  database: "${POSTGRES_DB}"
  user: "${POSTGRES_USER}"
  password: "${POSTGRES_PASSWORD}"
  source_schema: customer_app
  jdbc_driver: org.postgresql.Driver
spark:
  app_name: test
  config:
    shared: base
lakehouse:
  base_path: data/base
environment: base
"""

LOCAL_YAML = """
environment: local
spark:
  master: local[2]
  config:
    local-only: enabled
lakehouse:
  base_path: data/test-lakehouse
"""


def _write_config_files(tmp_path: Path) -> tuple[Path, Path]:
    base = tmp_path / "base.yaml"
    local = tmp_path / "local.yaml"
    base.write_text(BASE_YAML, encoding="utf-8")
    local.write_text(LOCAL_YAML, encoding="utf-8")
    return base, local


def _set_postgres_env(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "ecommerce",
        "POSTGRES_USER": "admin",
        "POSTGRES_PASSWORD": "secret value",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_load_config_merges_sections_and_hides_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base, local = _write_config_files(tmp_path)
    _set_postgres_env(monkeypatch)

    config = load_config(local, base_path=base, dotenv_path=tmp_path / "missing.env")

    assert config.environment == "local"
    assert config.spark.master == "local[2]"
    assert config.spark.max_parallel_tables == 1
    assert config.spark.config == {"shared": "base", "local-only": "enabled"}
    assert config.postgres.password_value == "secret value"
    assert "secret value" not in repr(config)


def test_missing_required_environment_variable_fails_fast(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base, local = _write_config_files(tmp_path)
    _set_postgres_env(monkeypatch)
    monkeypatch.delenv("POSTGRES_PASSWORD")

    with pytest.raises(ConfigError, match="POSTGRES_PASSWORD"):
        load_config(local, base_path=base, dotenv_path=tmp_path / "missing.env")


def test_invalid_port_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base, local = _write_config_files(tmp_path)
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv("POSTGRES_PORT", "70000")

    with pytest.raises(ConfigError, match="less than or equal to 65535"):
        load_config(local, base_path=base, dotenv_path=tmp_path / "missing.env")


def test_postgres_jdbc_safety_defaults_are_validated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base, local = _write_config_files(tmp_path)
    _set_postgres_env(monkeypatch)

    postgres = load_config(local, base_path=base, dotenv_path=tmp_path / "missing.env").postgres

    assert postgres.max_jdbc_partitions == 4
    assert postgres.max_events_per_batch == 500_000
    assert postgres.retry_attempts == 3
    assert "connectTimeout=15" in postgres.jdbc_url
    assert "socketTimeout=300" in postgres.jdbc_url
    assert "tcpKeepAlive=true" in postgres.jdbc_url


def test_lakehouse_path_preserves_cloud_uri() -> None:
    config = LakehouseConfig(base_path="abfss://lakehouse@account.dfs.core.windows.net/ecommerce")

    assert config.table_reference("silver", "orders").value == (
        "abfss://lakehouse@account.dfs.core.windows.net/ecommerce/silver/orders"
    )
    assert config.table_reference("bronze", "orders").value.endswith("/bronze/batch/orders")


def test_local_streaming_config_is_cloud_protocol_compatible(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_postgres_env(monkeypatch)

    config = load_config("local", dotenv_path=Path("/tmp/missing.env"))

    assert config.streaming.enabled is True
    assert config.streaming.kafka is not None
    assert config.streaming.kafka.topic is None
    assert config.streaming.kafka.topic_pattern == r"ecommerce\.customer_app\..*"
    assert config.streaming.kafka.subscribe_key == "subscribePattern"
    assert config.streaming.kafka.spark_options()["kafka.bootstrap.servers"] == "localhost:29092"
    assert config.streaming.trigger_interval == "2 seconds"
    assert config.streaming.checkpoint_location == "data/checkpoints/ecommerce-cdc-to-bronze/v2"
    assert config.streaming.silver.enabled is True
    assert config.streaming.silver.trigger_interval == "5 seconds"
    assert config.streaming.silver_checkpoint_location == "data/checkpoints/ecommerce-cdc-to-silver/v3"
    assert config.streaming.silver.max_files_per_trigger == 4
    assert config.streaming.silver.max_bytes_per_trigger == "32m"
    assert config.streaming.silver.reconcile_gold_each_batch is True
    assert config.streaming.silver.max_gold_readiness_order_ids == 5000
    assert config.streaming.silver.gold_deferred_retry_seconds == 30
    assert config.streaming.silver.gold_deferred_retry_interval_seconds == 2
    assert config.spark.config["spark.sql.debug.maxToStringFields"] == "2000"
    assert config.spark.config["spark.driver.extraJavaOptions"].endswith("infra/local/spark/log4j2.properties")
    assert config.lakehouse.streaming_bronze_reference("cdc_events").value.endswith("/bronze/streaming/cdc_events")
    assert config.lakehouse.streaming_typed_bronze_reference("orders").value.endswith("/bronze/streaming/orders")
    assert config.coordination.gold_owner == "streaming"


def test_checkpoint_location_preserves_cloud_uri() -> None:
    from ecommerce_pipeline.config.models import StreamingConfig, UnifiedSilverStreamingConfig

    streaming = StreamingConfig(
        enabled=True,
        checkpoint_root="abfss://lakehouse@account.dfs.core.windows.net/_checkpoints",
        kafka={
            "bootstrap_servers": "namespace.servicebus.windows.net:9093",
            "topic_pattern": r"ecommerce\.customer_app\..*",
        },
    )

    assert streaming.checkpoint_location == (
        "abfss://lakehouse@account.dfs.core.windows.net/_checkpoints/ecommerce-cdc-to-bronze/v1"
    )
    streaming = streaming.model_copy(update={"silver": UnifiedSilverStreamingConfig(enabled=True)})
    assert streaming.silver_checkpoint_location == (
        "abfss://lakehouse@account.dfs.core.windows.net/_checkpoints/ecommerce-cdc-to-silver/v1"
    )


def test_azure_config_uses_unity_catalog_identifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv("DATABRICKS_CATALOG", "catalog")
    monkeypatch.setenv("DATABRICKS_BRONZE_SCHEMA", "bronze")
    monkeypatch.setenv("DATABRICKS_SILVER_SCHEMA", "silver")
    monkeypatch.setenv("DATABRICKS_GOLD_SCHEMA", "gold")
    monkeypatch.setenv(
        "DATABRICKS_EXTERNAL_STORAGE_ROOT",
        "abfss://lakehouse@storage.dfs.core.windows.net/ecommerce/dev",
    )

    config = load_config("azure", dotenv_path=Path("/tmp/missing.env"))

    reference = config.lakehouse.table_reference("silver", "orders")
    assert reference.value == "catalog.silver.orders"
    assert reference.is_catalog is True
    assert reference.storage_path == ("abfss://lakehouse@storage.dfs.core.windows.net/ecommerce/dev/silver/orders")
    typed = config.lakehouse.streaming_typed_bronze_reference("orders")
    assert typed.value == "catalog.bronze.cdc_typed_orders"
    assert typed.storage_path.endswith("/bronze/streaming/orders")
    assert config.coordination.gold_owner == "batch"
    assert config.application.logs_path == ("/Workspace/Users/2251120184@ut.edu.vn/ecommerce-pipeline/logs")
    assert config.spark.master is None
    assert config.spark.configure_delta_package is False
    assert config.spark.stop_session is False
    assert not any(key.startswith("spark.hadoop.fs.azure.account") for key in config.spark.config)


def test_default_configs_have_one_project_source() -> None:
    assert loader.PROJECT_CONFIG_DIR == loader.PROJECT_ROOT / "configs"
    assert loader.DEFAULT_BASE_CONFIG == loader.PROJECT_CONFIG_DIR / "base.yaml"
