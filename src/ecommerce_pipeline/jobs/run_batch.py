from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from ecommerce_pipeline.batch.build_gold import build_gold
from ecommerce_pipeline.batch.build_silver import build_silver
from ecommerce_pipeline.batch.extract_to_bronze import extract_all_to_bronze
from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.control.batch_runs import (
    BronzeTableResult,
    batch_run_path,
    local_pipeline_lock,
    new_batch_id,
    write_batch_run_status,
)
from ecommerce_pipeline.runtime.spark import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch pipeline jobs.")
    parser.add_argument("--env", default="local", help="Config environment name or YAML path.")
    parser.add_argument("--mode", default="bronze", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--batch-id")
    parser.add_argument("--tables", nargs="*", help="Optional source table names to extract.")
    return parser.parse_args()


def run_mode(
    spark: SparkSession, args: argparse.Namespace, config: AppConfig, batch_id: str
) -> tuple[list[BronzeTableResult], dict[str, list[str]]]:
    bronze_results: list[BronzeTableResult] = []
    outputs: dict[str, list[str]] = {}
    if args.mode in {"bronze", "all"}:
        bronze_results = extract_all_to_bronze(spark, config, batch_id, args.tables)
        outputs["bronze"] = [result.output_path for result in bronze_results]
        for result in bronze_results:
            print(f"bronze table={result.table_name} records={result.record_count} path={result.output_path}")
        write_batch_run_status(
            config.lakehouse.base_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            spark=spark,
            outputs=outputs,
        )

    if args.mode in {"silver", "all"}:
        silver_tables = args.tables if args.mode == "silver" else None
        outputs["silver"] = build_silver(spark, config, silver_tables)
        for path in outputs["silver"]:
            print(f"silver path={path}")
        write_batch_run_status(
            config.lakehouse.base_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            spark=spark,
            outputs=outputs,
        )

    if args.mode in {"gold", "all"}:
        outputs["gold"] = build_gold(spark, config)
        for path in outputs["gold"]:
            print(f"gold path={path}")
        write_batch_run_status(
            config.lakehouse.base_path,
            batch_id,
            "RUNNING",
            config.application.timezone,
            results=bronze_results,
            spark=spark,
            outputs=outputs,
        )
    return bronze_results, outputs


def main() -> None:
    args = parse_args()
    if args.tables == []:
        raise SystemExit("--tables requires at least one table name")
    if args.tables and args.mode in {"gold", "all"}:
        raise SystemExit("--tables is supported only for bronze or silver mode")
    config = load_config(args.env)
    batch_id = args.batch_id or new_batch_id(config.application.timezone)
    batch_run_path(config.lakehouse.base_path, batch_id)
    spark: SparkSession | None = None
    with local_pipeline_lock(config.lakehouse.base_path, batch_id):
        try:
            spark = build_spark(config)
            write_batch_run_status(
                config.lakehouse.base_path, batch_id, "RUNNING", config.application.timezone, spark=spark
            )
            results, outputs = run_mode(spark, args, config, batch_id)
            summary_path = write_batch_run_status(
                config.lakehouse.base_path,
                batch_id,
                "SUCCEEDED",
                config.application.timezone,
                results=results,
                spark=spark,
                outputs=outputs,
            )
            print(f"batch_id={batch_id} summary={summary_path}")
        except Exception as exc:
            if spark is not None or "://" not in config.lakehouse.base_path:
                write_batch_run_status(
                    config.lakehouse.base_path,
                    batch_id,
                    "FAILED",
                    config.application.timezone,
                    error=str(exc),
                    spark=spark,
                )
            raise
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
