from __future__ import annotations

from pathlib import Path

import pytest

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
  format: delta
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

    assert config.table_path("silver", "orders") == (
        "abfss://lakehouse@account.dfs.core.windows.net/ecommerce/silver/orders"
    )


def test_azure_config_uses_adls_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_postgres_env(monkeypatch)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "acct")
    monkeypatch.setenv("AZURE_CONTAINER", "lakehouse")
    monkeypatch.setenv("AZURE_STORAGE_AUTH_TYPE", "account_key")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "secret-key")

    config = load_config("azure", dotenv_path=Path("/tmp/missing.env"))

    assert config.lakehouse.base_path == "abfss://lakehouse@acct.dfs.core.windows.net/lakehouse"
    assert config.spark.config["spark.hadoop.fs.azure.account.auth.type.acct.dfs.core.windows.net"] == "SharedKey"
    assert config.spark.config["spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net"] == "secret-key"
