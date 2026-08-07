from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from importlib import import_module
from time import perf_counter
from typing import Protocol, cast

from pydantic import SecretStr
from pyspark.sql import SparkSession

from ecommerce_pipeline.adapters.lakehouse import try_latest_delta_pipeline_commit
from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.control.batch_runs import local_pipeline_lock, new_batch_id, write_batch_run_status
from ecommerce_pipeline.control.cloud_lock import cloud_pipeline_lock
from ecommerce_pipeline.control.gold_releases import GoldReleaseStore
from ecommerce_pipeline.control.manifests import BronzeBatchManifest, BronzeTableResult, SilverBatchManifest
from ecommerce_pipeline.ingestion.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.pipelines.build_gold import build_gold
from ecommerce_pipeline.pipelines.build_silver import SILVER_DATA_PIPELINES, build_silver
from ecommerce_pipeline.runtime.spark import build_spark


class _DatabricksSecrets(Protocol):
    def get(self, scope: str, key: str) -> str: ...


class _DatabricksUtils(Protocol):
    secrets: _DatabricksSecrets


class _DBUtilsFactory(Protocol):
    def __call__(self, spark: SparkSession) -> _DatabricksUtils: ...


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch pipeline jobs.")
    parser.add_argument("--env", default="local", help="Config environment name or YAML path.")
    parser.add_argument("--base-config", help="Base YAML path; defaults to configs/base.yaml in the project.")
    parser.add_argument("--mode", default="bronze", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--batch-id")
    parser.add_argument("--tables", nargs="*", help="Optional source table names to extract.")
    parser.add_argument("--postgres-host")
    parser.add_argument("--postgres-port")
    parser.add_argument("--postgres-database")
    parser.add_argument("--postgres-user")
    parser.add_argument("--uc-catalog")
    parser.add_argument("--bronze-schema")
    parser.add_argument("--silver-schema")
    parser.add_argument("--gold-schema")
    parser.add_argument("--external-storage-root")
    parser.add_argument("--secret-scope")
    parser.add_argument(
        "--full-rebuild-silver",
        action="store_true",
        help="Explicitly rebuild Silver from complete Bronze history.",
    )
    parser.add_argument(
        "--full-rebuild-gold",
        action="store_true",
        help="Explicitly rebuild Gold from complete Silver snapshots.",
    )
    return parser.parse_args()


def _prepare_databricks_environment(args: argparse.Namespace) -> None:
    if not args.secret_scope:
        return
    values = {
        "POSTGRES_HOST": args.postgres_host,
        "POSTGRES_PORT": args.postgres_port,
        "POSTGRES_DB": args.postgres_database,
        "POSTGRES_USER": args.postgres_user,
        "DATABRICKS_CATALOG": args.uc_catalog,
        "DATABRICKS_BRONZE_SCHEMA": args.bronze_schema,
        "DATABRICKS_SILVER_SCHEMA": args.silver_schema,
        "DATABRICKS_GOLD_SCHEMA": args.gold_schema,
        "DATABRICKS_EXTERNAL_STORAGE_ROOT": args.external_storage_root,
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise SystemExit(f"Databricks task is missing parameters: {', '.join(missing)}")
    os.environ.update(cast(dict[str, str], values))
    os.environ["POSTGRES_PASSWORD"] = "__DATABRICKS_SECRET__"


def _databricks_utils(spark: SparkSession) -> _DatabricksUtils:
    try:
        module = import_module("pyspark.dbutils")
        factory = cast(_DBUtilsFactory, module.DBUtils)
        return factory(spark)
    except (ImportError, AttributeError, TypeError) as exc:
        raise RuntimeError("Databricks DBUtils is unavailable on this Spark runtime") from exc


def _apply_databricks_postgres_secret(
    spark: SparkSession,
    config: AppConfig,
    secret_scope: str | None,
) -> AppConfig:
    if not secret_scope:
        return config
    secrets = _databricks_utils(spark).secrets
    postgres_password = secrets.get(scope=secret_scope, key="postgres-password")
    postgres = config.postgres.model_copy(update={"password": SecretStr(postgres_password)})
    return config.model_copy(update={"postgres": postgres})


def run_mode(
    spark: SparkSession,
    args: argparse.Namespace,
    config: AppConfig,
    batch_id: str,
    timings_ms: dict[str, int],
) -> tuple[list[BronzeTableResult], dict[str, list[str]]]:
    bronze_results: list[BronzeTableResult] = []
    bronze_manifest: BronzeBatchManifest | None = None
    outputs: dict[str, list[str]] = {}
    if args.mode in {"bronze", "all"}:
        started = perf_counter()
        bronze_manifest = extract_all_to_bronze(
            spark,
            config,
            batch_id,
            args.tables,
            timings_ms=timings_ms,
        )
        bronze_results = bronze_manifest.results
        timings_ms["bronze"] = round((perf_counter() - started) * 1000)
        outputs["bronze"] = [result.output_path for result in bronze_results]
        changed = [f"{result.table_name}:{result.record_count}" for result in bronze_results if result.record_count]
        print(
            f"[bronze] tables={len(bronze_results)} records={sum(result.record_count for result in bronze_results)} "
            f"changed={','.join(changed) if changed else 'none'} elapsed={_seconds(timings_ms['bronze'])}",
            flush=True,
        )
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )
        if (
            args.mode == "all"
            and not args.full_rebuild_silver
            and not args.full_rebuild_gold
            and _is_no_change_batch(bronze_results)
            and _downstream_is_current(spark, config)
        ):
            print("[pipeline] no changes; skipped=silver,gold", flush=True)
            return bronze_results, outputs

    silver_manifest: SilverBatchManifest | None = None
    if args.mode in {"silver", "all"}:
        started = perf_counter()
        silver_tables = args.tables if args.mode == "silver" else None
        writer_lock = (
            cloud_pipeline_lock(spark, config, f"batch-{batch_id}")
            if getattr(getattr(config, "spark", None), "master", "local") is None
            else nullcontext()
        )
        with writer_lock:
            silver_manifest = build_silver(
                spark,
                config,
                silver_tables,
                batch_id=batch_id,
                full_rebuild=args.full_rebuild_silver,
                bronze_manifest=bronze_manifest if args.mode == "all" else None,
                timings_ms=timings_ms,
            )
        outputs["silver"] = silver_manifest.outputs
        timings_ms["silver"] = round((perf_counter() - started) * 1000)
        print(
            f"[silver] tables={len(outputs['silver'])} elapsed={_seconds(timings_ms['silver'])}",
            flush=True,
        )
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )

    gold_owner = getattr(getattr(config, "coordination", None), "gold_owner", "batch")
    if args.mode == "gold" and gold_owner != "batch":
        raise RuntimeError(
            "Gold ownership is assigned to streaming; batch --mode gold is disabled to prevent dual writers"
        )
    if args.mode in {"gold", "all"} and gold_owner == "batch":
        started = perf_counter()
        outputs["gold"] = build_gold(
            spark,
            config,
            batch_id=batch_id,
            full_rebuild=args.full_rebuild_gold,
            timings_ms=timings_ms,
            silver_manifest=silver_manifest,
        )
        timings_ms["gold"] = round((perf_counter() - started) * 1000)
        print(
            f"[gold] tables={len(outputs['gold'])} elapsed={_seconds(timings_ms['gold'])}",
            flush=True,
        )
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )
    elif args.mode == "all":
        print("[gold] skipped owner=streaming; batch pipeline stops at Unified Silver", flush=True)
    return bronze_results, outputs


def _is_no_change_batch(results: list[BronzeTableResult]) -> bool:
    return bool(results) and all(
        result.ingestion_type == "incremental" and result.record_count == 0 for result in results
    )


def _downstream_is_current(spark: SparkSession, config: AppConfig) -> bool:
    """Skip safely only when Unified Silver exists and the active Gold release has consumed it."""

    release = GoldReleaseStore(spark, config).latest()
    if release is None or set(release.silver_versions) != set(SILVER_TABLES):
        return False

    def inspect_table(table_name: str) -> tuple[str, int | None]:
        session = spark.newSession()
        commit = try_latest_delta_pipeline_commit(
            session,
            config.lakehouse.table_reference("silver", table_name),
            pipelines=SILVER_DATA_PIPELINES,
        )
        if commit is None or commit.metadata.get("silver_schema_version") != 2:
            return table_name, None
        return table_name, commit.version

    workers = max(1, min(len(SILVER_TABLES), config.spark.max_parallel_tables))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        inspected = dict(executor.map(inspect_table, SILVER_TABLES))
    return None not in inspected.values() and release.silver_versions == inspected


def _seconds(milliseconds: int) -> str:
    return f"{milliseconds / 1000:.2f}s"


def main() -> None:
    args = parse_args()
    _prepare_databricks_environment(args)
    if args.tables == []:
        raise SystemExit("--tables requires at least one table name")
    if args.tables and args.mode in {"gold", "all"}:
        raise SystemExit("--tables is supported only for bronze or silver mode")
    if args.full_rebuild_silver and args.mode not in {"silver", "all"}:
        raise SystemExit("--full-rebuild-silver requires --mode silver or --mode all")
    if args.full_rebuild_gold and args.mode not in {"gold", "all"}:
        raise SystemExit("--full-rebuild-gold requires --mode gold or --mode all")
    config = load_config(
        args.env,
        **({"base_path": args.base_config} if args.base_config else {}),
    )
    print(
        f"[config] env={config.environment} lakehouse={config.lakehouse.catalog or config.lakehouse.base_path} "
        f"logs={config.application.logs_path}",
        flush=True,
    )
    batch_id = args.batch_id or new_batch_id(config.application.timezone)
    spark: SparkSession | None = None
    run_started = perf_counter()
    timings_ms: dict[str, int] = {}
    pipeline_lock = (
        local_pipeline_lock(config.application.logs_path, batch_id) if config.spark.master else nullcontext()
    )
    with pipeline_lock:
        try:
            spark_started = perf_counter()
            spark = build_spark(config)
            config = _apply_databricks_postgres_secret(
                spark,
                config,
                args.secret_scope,
            )
            timings_ms["spark_startup"] = round((perf_counter() - spark_started) * 1000)
            write_batch_run_status(
                config.application.logs_path,
                batch_id,
                "RUNNING",
                config.application.timezone,
                timings_ms=timings_ms,
            )
            results, outputs = run_mode(spark, args, config, batch_id, timings_ms)
            timings_ms["total"] = round((perf_counter() - run_started) * 1000)
            write_batch_run_status(
                config.application.logs_path,
                batch_id,
                "SUCCEEDED",
                config.application.timezone,
                results=results,
                outputs=outputs,
                timings_ms=timings_ms,
            )
        except Exception as exc:
            timings_ms["total"] = round((perf_counter() - run_started) * 1000)
            write_batch_run_status(
                config.application.logs_path,
                batch_id,
                "FAILED",
                config.application.timezone,
                error=str(exc),
                timings_ms=timings_ms,
            )
            raise
        finally:
            if spark is not None and config.spark.stop_session:
                spark.stop()


if __name__ == "__main__":
    main()
