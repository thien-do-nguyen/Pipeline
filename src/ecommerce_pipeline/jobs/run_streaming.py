from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import SparkSession
from pyspark.sql.streaming.query import StreamingQuery

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.ingestion.streaming.bronze import start_bronze_stream
from ecommerce_pipeline.ingestion.streaming.debezium import normalize_debezium_events
from ecommerce_pipeline.ingestion.streaming.kafka_source import read_kafka_stream
from ecommerce_pipeline.ingestion.streaming.state import assert_local_delta_target_matches_checkpoints
from ecommerce_pipeline.runtime.shutdown import log_runtime_failure, stop_spark_safely, stop_streaming_query_safely
from ecommerce_pipeline.runtime.spark import build_spark
from ecommerce_pipeline.runtime.streaming_progress import summarize_streaming_progress


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Debezium CDC events from Kafka into raw Bronze Delta")
    parser.add_argument("--env", default="configs/local.yaml", help="Environment name or YAML overlay")
    parser.add_argument("--base-config", help="Optional base YAML path")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process all currently available offsets and stop instead of running continuously",
    )
    return parser.parse_args(argv)


def start_query(spark: SparkSession, config: AppConfig, *, available_now: bool) -> StreamingQuery:
    streaming = config.streaming
    if not streaming.enabled or streaming.kafka is None:
        raise RuntimeError(f"Streaming is not enabled for environment: {config.environment}")

    reference = config.lakehouse.raw_cdc_bronze_reference(streaming.bronze_table)
    assert_local_delta_target_matches_checkpoints(
        reference,
        (streaming.checkpoint_location,),
        target_label="Raw CDC Bronze",
    )
    events = normalize_debezium_events(
        read_kafka_stream(spark, streaming.kafka),
        source_id=streaming.kafka.source_id,
    )
    query = start_bronze_stream(events, reference, streaming, available_now=available_now)
    print(
        f"[streaming] query={streaming.query_name} {streaming.kafka.subscribe_key}={streaming.kafka.subscription} "
        f"target={reference.value} checkpoint={streaming.checkpoint_location}",
        flush=True,
    )
    return query


def run(spark: SparkSession, config: AppConfig, *, available_now: bool) -> dict[str, int | float] | None:
    query = start_query(spark, config, available_now=available_now)
    try:
        query.awaitTermination()
        return summarize_streaming_progress(query) if available_now else None
    except KeyboardInterrupt:
        print("[streaming] status=stopping reason=keyboard_interrupt", flush=True)
    except Exception as exc:
        log_runtime_failure("streaming", exc)
        raise
    finally:
        stop_streaming_query_safely(query, label="streaming")
    return None


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    config = load_config(args.env, **({"base_path": args.base_config} if args.base_config else {}))
    streaming = config.streaming
    if not streaming.enabled:
        raise RuntimeError(f"Streaming is not enabled for environment: {config.environment}")

    spark: SparkSession | None = None
    try:
        spark = build_spark(config, extra_packages=streaming.spark_packages)
        run(spark, config, available_now=args.available_now)
    finally:
        if spark is not None and config.spark.stop_session:
            stop_spark_safely(spark, label="streaming")


if __name__ == "__main__":
    main()
