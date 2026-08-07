from __future__ import annotations

import argparse
from collections.abc import Sequence

from pyspark.sql import SparkSession

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.control.batch_runs import new_batch_id
from ecommerce_pipeline.ingestion.streaming.unified_silver import UnifiedSilverMaterializer
from ecommerce_pipeline.runtime.spark import build_spark


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile streaming-owned Gold from Unified Silver")
    parser.add_argument("--env", default="configs/local.yaml", help="Environment name or YAML overlay")
    parser.add_argument("--base-config", help="Optional base YAML path")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    config = load_config(args.env, **({"base_path": args.base_config} if args.base_config else {}))
    if config.coordination.gold_owner != "streaming":
        raise RuntimeError("Gold reconciliation requires coordination.gold_owner=streaming")
    spark: SparkSession | None = None
    try:
        spark = build_spark(config)
        batch_id = f"cdc-gold-{new_batch_id(config.application.timezone)}"
        UnifiedSilverMaterializer(spark, config).reconcile_gold(batch_id=batch_id)
    finally:
        if spark is not None and config.spark.stop_session:
            spark.stop()


if __name__ == "__main__":
    main()
