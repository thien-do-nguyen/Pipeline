from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from time import sleep

from dotenv import load_dotenv

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.validation.cdc import validate_postgres_cdc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate PostgreSQL CDC runtime state")
    parser.add_argument("--env", default="configs/local.yaml")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--cdc-user", default="ecommerce_cdc")
    parser.add_argument("--publication", default="ecommerce_cdc_publication")
    parser.add_argument("--slot", default="ecommerce_cdc_local")
    parser.add_argument("--max-heartbeat-age-seconds", type=int, default=60)
    parser.add_argument("--max-wal-retained-bytes", type=int, default=1_073_741_824)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-seconds", type=float, default=5.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    load_dotenv(args.env_file, override=False)
    password = os.getenv("CDC_POSTGRES_PASSWORD", "").strip()
    if not password:
        raise RuntimeError("CDC_POSTGRES_PASSWORD is required")
    if args.attempts < 1:
        raise ValueError("--attempts must be at least 1")
    report = None
    for attempt in range(1, args.attempts + 1):
        try:
            report = validate_postgres_cdc(
                load_config(args.env),
                cdc_user=args.cdc_user,
                cdc_password=password,
                publication=args.publication,
                slot=args.slot,
                max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
                max_wal_retained_bytes=args.max_wal_retained_bytes,
            )
            break
        except RuntimeError as exc:
            if attempt == args.attempts:
                raise
            print(f"[cdc-health] status=waiting attempt={attempt}/{args.attempts} error={exc}", flush=True)
            sleep(args.retry_seconds)
    assert report is not None
    print(
        f"[cdc-health] status=passed report={json.dumps(report.__dict__, separators=(',', ':'), sort_keys=True)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
