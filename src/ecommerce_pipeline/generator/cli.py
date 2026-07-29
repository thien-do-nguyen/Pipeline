import argparse

from .models import SeedPlan
from .scenarios import seed_continuous, seed_once


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate OLTP ecommerce data.")
    parser.add_argument("--config", default="configs/local.yaml")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--continuous", action="store_true", help="Keep creating orders and updates for CDC demos.")
    parser.add_argument("--orders-per-batch", type=int, default=5)
    parser.add_argument("--interval-seconds", type=float, default=10.0)
    parser.add_argument("--max-batches", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        plan = SeedPlan(customers=args.customers, orders=args.orders)
        if args.continuous:
            if args.reset:
                seed_once(args.config, plan, args.seed, reset=True, batch_size=args.batch_size)
            seed_continuous(args.config, args.seed, args.orders_per_batch, args.interval_seconds, args.max_batches)
        else:
            seed_once(args.config, plan, args.seed, args.reset, batch_size=args.batch_size)
    except ValueError as exc:
        raise SystemExit(f"Invalid generator arguments:\n{exc}") from exc


if __name__ == "__main__":
    main()
