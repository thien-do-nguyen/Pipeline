from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from importlib import import_module
from time import perf_counter
from typing import Protocol, cast

from pydantic import SecretStr
from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.cdc_routing import cdc_topic_pattern
from ecommerce_pipeline.jobs.run_silver_streaming import (
    run as run_silver_available_now,
)
from ecommerce_pipeline.jobs.run_silver_streaming import (
    start_query as start_silver_query,
)
from ecommerce_pipeline.jobs.run_streaming import run as run_raw_available_now
from ecommerce_pipeline.jobs.run_streaming import start_query as start_raw_query
from ecommerce_pipeline.runtime.shutdown import stop_streaming_query_safely
from ecommerce_pipeline.runtime.spark import build_spark
from ecommerce_pipeline.validation.cloud_cdc import validate_cloud_cdc_lakehouse


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
    parser.add_argument("--validation-mode", choices=("runtime", "canary"), default="runtime")
    parser.add_argument(
        "--execution-mode",
        choices=("available_now", "continuous"),
        default="available_now",
    )
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
    checkpoint_root = "/".join((args.external_storage_root.rstrip("/"), "_checkpoints", "cdc", namespace))
    streaming = config.streaming.model_copy(update={"checkpoint_root": checkpoint_root, "kafka": cloud_kafka})
    postgres = config.postgres.model_copy(update={"password": SecretStr(postgres_password)})
    return config.model_copy(update={"streaming": streaming, "postgres": postgres})


def run_cycle(spark: SparkSession, config: AppConfig, *, validation_mode: str = "runtime") -> None:
    kafka = config.streaming.kafka
    if kafka is None:
        raise RuntimeError("Cloud Kafka/Event Hubs configuration is missing")
    print(
        f"[cloud-cdc] stage=raw_bronze status=STARTING source_id={kafka.source_id} "
        f"checkpoint_root={config.streaming.checkpoint_root}",
        flush=True,
    )
    cycle_started = perf_counter()
    raw_started = perf_counter()
    raw_metrics = run_raw_available_now(spark, config, available_now=True)
    raw_elapsed_ms = round((perf_counter() - raw_started) * 1000)
    print("[cloud-cdc] stage=raw_bronze status=SUCCEEDED", flush=True)
    silver_started = perf_counter()
    silver_metrics = run_silver_available_now(spark, config, available_now=True)
    silver_elapsed_ms = round((perf_counter() - silver_started) * 1000)
    print("[cloud-cdc] stage=unified_silver_gold status=SUCCEEDED", flush=True)
    runtime_metrics = {
        "raw": raw_metrics or {},
        "silver_gold": silver_metrics or {},
        "raw_elapsed_ms": raw_elapsed_ms,
        "silver_gold_elapsed_ms": silver_elapsed_ms,
        "cycle_elapsed_ms": round((perf_counter() - cycle_started) * 1000),
    }
    print(f"[cdc-runtime-metrics] {json.dumps(runtime_metrics, separators=(',', ':'), sort_keys=True)}", flush=True)
    _emit_runtime_alerts(runtime_metrics)
    if validation_mode == "canary":
        report = validate_cloud_cdc_lakehouse(spark, config)
        _emit_canary_alerts(report.as_dict(), raw_metrics=raw_metrics or {})
        print(
            f"[cdc-canary] status=PASSED report={json.dumps(report.as_dict(), separators=(',', ':'), sort_keys=True)}",
            flush=True,
        )


def run_continuous(spark: SparkSession, config: AppConfig) -> None:
    """Keep Raw and downstream CDC queries alive in one restartable job task."""

    raw_query: StreamingQuery | None = None
    silver_query: StreamingQuery | None = None
    last_reconciled_batch: object | None = None
    try:
        raw_query = start_raw_query(spark, config, available_now=False)
        silver_query, materializer = start_silver_query(spark, config, available_now=False)
        print(
            "[cloud-cdc] mode=continuous status=RUNNING queries=raw_bronze,unified_silver_gold",
            flush=True,
        )
        interval = config.streaming.silver.gold_reconcile_interval_seconds
        while raw_query.isActive and silver_query.isActive:
            if raw_query.awaitTermination(1):
                break
            if silver_query.awaitTermination(max(1, round(interval))):
                break
            progress = silver_query.lastProgress
            if progress is None:
                continue
            batch_id = progress.get("batchId")
            if batch_id is None or batch_id == last_reconciled_batch:
                continue
            last_reconciled_batch = batch_id
            status = materializer.reconcile_pending_gold_if_ready(batch_id=f"cdc-continuous-{batch_id}")
            input_rows = int(progress.get("numInputRows", 0))
            duration = progress.get("durationMs", {})
            trigger_ms = duration.get("triggerExecution", 0) if isinstance(duration, dict) else 0
            print(
                f"[cdc-continuous-progress] batch={batch_id} input_rows={input_rows} "
                f"trigger_ms={trigger_ms} gold_status={status}",
                flush=True,
            )
        _raise_terminated_query(raw_query, "raw_bronze")
        _raise_terminated_query(silver_query, "unified_silver_gold")
    finally:
        if silver_query is not None:
            stop_streaming_query_safely(silver_query, label="cloud-unified-silver")
        if raw_query is not None:
            stop_streaming_query_safely(raw_query, label="cloud-raw-bronze")


def _raise_terminated_query(query: StreamingQuery, label: str) -> None:
    error = query.exception()
    if error is not None:
        raise RuntimeError(f"Cloud CDC query failed: {label}") from error
    raise RuntimeError(f"Cloud CDC query stopped unexpectedly: {label}")


def _emit_runtime_alerts(metrics: dict[str, object]) -> None:
    raw = metrics.get("raw")
    silver = metrics.get("silver_gold")
    for name, value, threshold in (
        (
            "event_hubs_lag",
            raw.get("latest_offsets_behind_latest", 0) if isinstance(raw, dict) else 0,
            0,
        ),
        ("raw_micro_batch_duration_ms", raw.get("max_trigger_ms", 0) if isinstance(raw, dict) else 0, 120_000),
        (
            "silver_micro_batch_duration_ms",
            silver.get("max_trigger_ms", 0) if isinstance(silver, dict) else 0,
            120_000,
        ),
    ):
        if float(str(value)) > threshold:
            print(f"[cdc-alert] metric={name} value={value} threshold={threshold}", flush=True)


def _emit_canary_alerts(report: dict[str, object], *, raw_metrics: Mapping[str, int | float]) -> None:
    input_rows = int(str(raw_metrics.get("input_rows", 0) or 0))
    lag = float(str(raw_metrics.get("latest_offsets_behind_latest", 0) or 0))
    freshness_checks = (
        (
            ("raw_freshness_seconds", report.get("raw_freshness_seconds"), 300),
            ("silver_freshness_seconds", report.get("silver_freshness_seconds"), 300),
            ("gold_freshness_seconds", report.get("gold_freshness_seconds"), 300),
        )
        if input_rows > 0 or lag > 0
        else ()
    )
    checks = freshness_checks + (
        ("pending_gold_count", report.get("pending_gold_count"), 0),
        ("pending_gold_oldest_age_seconds", report.get("pending_gold_oldest_age_seconds"), 60),
        ("quarantine_count", report.get("quarantine_count"), 0),
    )
    for metric, value, threshold in checks:
        if value is not None and float(str(value)) > threshold:
            print(f"[cdc-alert] metric={metric} value={value} threshold={threshold}", flush=True)


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
        if args.execution_mode == "continuous":
            run_continuous(spark, cloud_config)
        else:
            run_cycle(spark, cloud_config, validation_mode=args.validation_mode)
    except Exception as exc:
        print(
            f"[cdc-alert] metric=cdc_cycle_failure value=1 error_type={type(exc).__name__} error={exc}",
            flush=True,
        )
        raise
    finally:
        if spark is not None and config.spark.stop_session:
            spark.stop()


if __name__ == "__main__":
    main()
