from __future__ import annotations

import argparse
from time import perf_counter

from pyspark.sql import SparkSession

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.control.batch_runs import (
    BronzeTableResult,
    local_pipeline_lock,
    new_batch_id,
    write_batch_run_status,
)
from ecommerce_pipeline.ingestion.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.pipelines.build_gold import build_gold
from ecommerce_pipeline.pipelines.build_silver import build_silver
from ecommerce_pipeline.runtime.spark import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch pipeline jobs.")
    parser.add_argument("--env", default="local", help="Config environment name or YAML path.")
    parser.add_argument("--mode", default="bronze", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--batch-id")
    parser.add_argument("--tables", nargs="*", help="Optional source table names to extract.")
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


def run_mode(
    spark: SparkSession,
    args: argparse.Namespace,
    config: AppConfig,
    batch_id: str,
    timings_ms: dict[str, int],
) -> tuple[list[BronzeTableResult], dict[str, list[str]]]:
    bronze_results: list[BronzeTableResult] = []
    outputs: dict[str, list[str]] = {}
    if args.mode in {"bronze", "all"}:
        started = perf_counter()
        bronze_results = extract_all_to_bronze(spark, config, batch_id, args.tables)
        timings_ms["bronze"] = round((perf_counter() - started) * 1000)
        outputs["bronze"] = [result.output_path for result in bronze_results]
        for result in bronze_results:
            print(f"bronze table={result.table_name} records={result.record_count} path={result.output_path}")
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )

    if args.mode in {"silver", "all"}:
        started = perf_counter()
        silver_tables = args.tables if args.mode == "silver" else None
        outputs["silver"] = build_silver(
            spark,
            config,
            silver_tables,
            batch_id=batch_id,
            full_rebuild=args.full_rebuild_silver,
        )
        timings_ms["silver"] = round((perf_counter() - started) * 1000)
        for path in outputs["silver"]:
            print(f"silver path={path}")
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )

    if args.mode in {"gold", "all"}:
        started = perf_counter()
        outputs["gold"] = build_gold(
            spark,
            config,
            batch_id=batch_id,
            full_rebuild=args.full_rebuild_gold,
            timings_ms=timings_ms,
        )
        timings_ms["gold"] = round((perf_counter() - started) * 1000)
        for path in outputs["gold"]:
            print(f"gold path={path}")
        write_batch_run_status(
            config.application.logs_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            outputs=outputs,
            timings_ms=timings_ms,
        )
    return bronze_results, outputs


def main() -> None:
    args = parse_args()
    if args.tables == []:
        raise SystemExit("--tables requires at least one table name")
    if args.tables and args.mode in {"gold", "all"}:
        raise SystemExit("--tables is supported only for bronze or silver mode")
    if args.full_rebuild_silver and args.mode not in {"silver", "all"}:
        raise SystemExit("--full-rebuild-silver requires --mode silver or --mode all")
    if args.full_rebuild_gold and args.mode not in {"gold", "all"}:
        raise SystemExit("--full-rebuild-gold requires --mode gold or --mode all")
    config = load_config(args.env)
    batch_id = args.batch_id or new_batch_id(config.application.timezone)
    spark: SparkSession | None = None
    run_started = perf_counter()
    timings_ms: dict[str, int] = {}
    with local_pipeline_lock(config.application.logs_path, batch_id):
        try:
            spark_started = perf_counter()
            spark = build_spark(config)
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
            summary_path = write_batch_run_status(
                config.application.logs_path,
                batch_id,
                "SUCCEEDED",
                config.application.timezone,
                results=results,
                outputs=outputs,
                timings_ms=timings_ms,
            )
            print(f"batch_id={batch_id} summary={summary_path}")
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
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
