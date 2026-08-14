from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from importlib import import_module
from typing import Protocol, cast

from pydantic import SecretStr
from pyspark.sql import SparkSession

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.cdc_routing import cdc_topic_pattern
from ecommerce_pipeline.jobs.run_silver_streaming import run as run_silver_available_now
from ecommerce_pipeline.jobs.run_streaming import run as run_raw_available_now
from ecommerce_pipeline.runtime.spark import build_spark


class _DatabricksSecrets(Protocol):
    def get(self, scope: str, key: str) -> str: ...


class _DatabricksUtils(Protocol):
    secrets: _DatabricksSecrets


class _DBUtilsFactory(Protocol):
    def __call__(self, spark: SparkSession) -> _DatabricksUtils: ...


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one cloud CDC available-now cycle on shared Databricks compute")
    parser.add_argument("--env", required=True)
    parser.add_argument("--base-config", required=True)
    parser.add_argument(
        "--event-hubs-namespace",
        required=True,
        help="Event Hubs namespace containing the domain CDC topics",
    )
    parser.add_argument("--event-hubs-secret-prefix", default="event-hubs-listen-connection-string")
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--postgres-host", required=True)
    parser.add_argument("--postgres-port", required=True)
    parser.add_argument("--postgres-database", required=True)
    parser.add_argument("--postgres-user", required=True)
    parser.add_argument("--uc-catalog", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--external-storage-root", required=True)
    return parser.parse_args(argv)


def _prepare_environment(args: argparse.Namespace) -> None:
    values = {
        "POSTGRES_HOST": args.postgres_host,
        "POSTGRES_PORT": args.postgres_port,
        "POSTGRES_DB": args.postgres_database,
        "POSTGRES_USER": args.postgres_user,
        "POSTGRES_PASSWORD": "__DATABRICKS_SECRET__",
        "DATABRICKS_CATALOG": args.uc_catalog,
        "DATABRICKS_BRONZE_SCHEMA": args.bronze_schema,
        "DATABRICKS_SILVER_SCHEMA": args.silver_schema,
        "DATABRICKS_GOLD_SCHEMA": args.gold_schema,
        "DATABRICKS_EXTERNAL_STORAGE_ROOT": args.external_storage_root,
    }
    os.environ.update(cast(dict[str, str], values))


def _dbutils(spark: SparkSession) -> _DatabricksUtils:
    try:
        factory = cast(_DBUtilsFactory, import_module("pyspark.dbutils").DBUtils)
        return factory(spark)
    except (ImportError, AttributeError, TypeError) as exc:
        raise RuntimeError("Databricks DBUtils is unavailable on this Spark runtime") from exc


def _event_hubs_jaas(connection_string: str) -> str:
    """Build SASL/PLAIN JAAS for Databricks' shaded Kafka client."""

    return (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )


def _apply_cloud_secrets(
    spark: SparkSession,
    config: AppConfig,
    args: argparse.Namespace,
    *,
    namespace: str,
) -> AppConfig:
    secrets = _dbutils(spark).secrets
    event_hubs_connection = secrets.get(
        scope=args.secret_scope,
        key=args.event_hubs_secret_prefix,
    )
    postgres_password = secrets.get(scope=args.secret_scope, key="postgres-password")
    kafka = config.streaming.kafka
    if kafka is None:
        raise RuntimeError("Azure streaming.kafka configuration is missing")
    jaas = _event_hubs_jaas(event_hubs_connection)
    cloud_kafka = kafka.model_copy(
        update={
            "source_id": namespace,
            "bootstrap_servers": f"{namespace}.servicebus.windows.net:9093",
            "topic_pattern": cdc_topic_pattern(),
            "secret_options": {"kafka.sasl.jaas.config": SecretStr(jaas)},
        }
    )
    checkpoint_root = "/".join(
        (args.external_storage_root.rstrip("/"), "_checkpoints", "cdc", namespace)
    )
    streaming = config.streaming.model_copy(update={"checkpoint_root": checkpoint_root, "kafka": cloud_kafka})
    postgres = config.postgres.model_copy(update={"password": SecretStr(postgres_password)})
    return config.model_copy(update={"streaming": streaming, "postgres": postgres})


def run_cycle(spark: SparkSession, config: AppConfig) -> None:
    kafka = config.streaming.kafka
    if kafka is None:
        raise RuntimeError("Cloud Kafka/Event Hubs configuration is missing")
    print(
        f"[cloud-cdc] stage=raw_bronze status=STARTING source_id={kafka.source_id} "
        f"checkpoint_root={config.streaming.checkpoint_root}",
        flush=True,
    )
    run_raw_available_now(spark, config, available_now=True)
    print("[cloud-cdc] stage=raw_bronze status=SUCCEEDED", flush=True)
    run_silver_available_now(spark, config, available_now=True)
    print("[cloud-cdc] stage=unified_silver_gold status=SUCCEEDED", flush=True)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    namespace = args.event_hubs_namespace.strip()
    if not namespace:
        raise ValueError("--event-hubs-namespace must not be empty")
    _prepare_environment(args)
    config = load_config(args.env, base_path=args.base_config)
    spark: SparkSession | None = None
    try:
        spark = build_spark(config)
        cloud_config = _apply_cloud_secrets(spark, config, args, namespace=namespace)
        run_cycle(spark, cloud_config)
    finally:
        if spark is not None and config.spark.stop_session:
            spark.stop()


if __name__ == "__main__":
    main()
