from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.ingestion.streaming.bronze import ensure_bronze_target
from ecommerce_pipeline.ingestion.streaming.cdc_decoder import read_raw_bronze_stream
from ecommerce_pipeline.ingestion.streaming.debezium import empty_normalized_debezium_events
from ecommerce_pipeline.ingestion.streaming.state import assert_local_delta_target_matches_checkpoints
from ecommerce_pipeline.ingestion.streaming.unified_silver import (
    UnifiedSilverMaterializer,
    start_unified_silver_stream,
)
from ecommerce_pipeline.runtime.spark import build_spark

PROGRESS_LOG_INTERVAL_SECONDS = 10
NO_INPUT_PROGRESS_LOG_EVERY_BATCHES = 10


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge raw CDC Bronze into shared Silver and Gold")
    parser.add_argument("--env", default="configs/local.yaml", help="Environment name or YAML overlay")
    parser.add_argument("--base-config", help="Optional base YAML path")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process the raw Bronze backlog and stop instead of running continuously",
    )
    return parser.parse_args(argv)


def run(spark: SparkSession, config: AppConfig, *, available_now: bool) -> None:
    settings = config.streaming.silver
    if not settings.enabled:
        raise RuntimeError(f"Streaming Silver is not enabled for environment: {config.environment}")

    raw_reference = config.lakehouse.streaming_bronze_reference(config.streaming.bronze_table)
    assert_local_delta_target_matches_checkpoints(
        raw_reference,
        (config.streaming.checkpoint_location, config.streaming.silver_checkpoint_location),
        target_label="Raw CDC Bronze",
    )
    ensure_bronze_target(empty_normalized_debezium_events(spark), raw_reference)
    materializer = UnifiedSilverMaterializer(spark, config)
    query = start_unified_silver_stream(
        read_raw_bronze_stream(
            spark,
            raw_reference,
            max_files_per_trigger=settings.max_files_per_trigger,
            max_bytes_per_trigger=settings.max_bytes_per_trigger,
        ),
        materializer,
        settings,
        config.streaming.silver_checkpoint_location,
        available_now=available_now,
    )
    print(
        f"[unified-silver] query={settings.query_name} source={raw_reference.value} "
        f"targets=bronze/streaming/<typed-table>,silver gold_each_batch={settings.reconcile_gold_each_batch} "
        f"checkpoint={config.streaming.silver_checkpoint_location}",
        flush=True,
    )
    if available_now:
        query.awaitTermination()
        materializer.reconcile_gold()
        return
    _await_continuous_query(query, materializer)


def _await_continuous_query(query: StreamingQuery, materializer: UnifiedSilverMaterializer) -> None:
    last_progress_key: tuple[object, object] | None = None
    while query.isActive:
        terminated = query.awaitTermination(PROGRESS_LOG_INTERVAL_SECONDS)
        progress = query.lastProgress
        if progress is not None:
            progress_key = (progress.get("runId"), progress.get("batchId"))
            if progress_key != last_progress_key and _should_log_progress(progress):
                last_progress_key = progress_key
                print(_format_progress(progress), flush=True)
        elif not terminated:
            print("[unified-silver-progress] status=waiting_for_raw_bronze input_rows=0", flush=True)
        if not terminated:
            materializer.reconcile_pending_gold_if_ready(batch_id="cdc-idle-reconcile")
        if terminated:
            return


def _format_progress(progress: dict[str, Any]) -> str:
    input_rows = int(progress.get("numInputRows", 0))
    if input_rows == 0:
        return f"[unified-silver-progress] status=waiting_for_raw_bronze batch={progress.get('batchId')} input_rows=0"
    duration_ms = progress.get("durationMs")
    trigger_ms = duration_ms.get("triggerExecution") if isinstance(duration_ms, dict) else None
    processed_rows_per_second = progress.get("processedRowsPerSecond", 0.0)
    return (
        f"[unified-silver-progress] batch={progress.get('batchId')} "
        f"input_rows={input_rows} "
        f"processed_rows_per_second={float(processed_rows_per_second):.2f} "
        f"trigger_ms={trigger_ms if trigger_ms is not None else 'unknown'}"
    )


def _should_log_progress(progress: dict[str, Any]) -> bool:
    input_rows = int(progress.get("numInputRows", 0))
    if input_rows > 0:
        return True
    batch_id = progress.get("batchId")
    return isinstance(batch_id, int) and batch_id % NO_INPUT_PROGRESS_LOG_EVERY_BATCHES == 0


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    config = load_config(args.env, **({"base_path": args.base_config} if args.base_config else {}))
    spark: SparkSession | None = None
    try:
        spark = build_spark(config)
        run(spark, config, available_now=args.available_now)
    finally:
        if spark is not None and config.spark.stop_session:
            spark.stop()


if __name__ == "__main__":
    main()
