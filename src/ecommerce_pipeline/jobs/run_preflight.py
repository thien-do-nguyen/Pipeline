from __future__ import annotations

import argparse
import json

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.validation.preflight import check_postgres_source


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only local pipeline preflight checks.")
    parser.add_argument("--check", required=True, choices=["postgres"])
    parser.add_argument("--env", default="local", help="Config environment name or YAML path.")
    parser.add_argument("--base-config", help="Base YAML path; defaults to configs/base.yaml in the project.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.env, **({"base_path": args.base_config} if args.base_config else {}))
    report = check_postgres_source(config)
    print(f"[preflight] check={args.check} status=passed report={json.dumps(report, separators=(',', ':'))}")


if __name__ == "__main__":
    main()
