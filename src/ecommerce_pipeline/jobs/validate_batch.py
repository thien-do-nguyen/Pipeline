from __future__ import annotations

import argparse
import json

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.runtime.spark import build_spark
from ecommerce_pipeline.validation.batch import validate_batch_lakehouse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the local batch lakehouse.")
    parser.add_argument("--env", default="local", help="Config environment name or YAML path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.env)
    spark = build_spark(config)
    try:
        report = validate_batch_lakehouse(spark, config)
        print(json.dumps(report.as_dict(), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
