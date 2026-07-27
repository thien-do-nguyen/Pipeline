import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ecommerce_pipeline.config.loader import load_config

from .catalog import load_state, seed_baseline
from .database import connect, reset_source
from .models import SeedPlan, StreamPlan
from .orders import apply_change_events, insert_order


def seed_once(config_path: str, plan: SeedPlan, seed: int, reset: bool) -> None:
    rng = random.Random(seed)
    config = load_config(config_path)
    now = _now(config.application.timezone)

    with connect(config.postgres) as conn:
        with conn.cursor() as cur:
            if reset:
                reset_source(cur)
            state = seed_baseline(cur, rng, plan.customers, now)
            for idx in range(1, plan.orders + 1):
                created_at = now - timedelta(days=rng.randint(0, 90), minutes=rng.randint(0, 1440))
                insert_order(cur, rng, state, f"ORD-{idx:06d}", created_at)
        conn.commit()


def seed_continuous(
    config_path: str,
    seed: int,
    orders_per_batch: int,
    interval_seconds: float,
    max_batches: int | None,
) -> None:
    plan = StreamPlan(
        orders_per_batch=orders_per_batch,
        interval_seconds=interval_seconds,
        max_batches=max_batches,
    )
    rng = random.Random(seed)
    config = load_config(config_path)
    batch = 0

    while plan.max_batches is None or batch < plan.max_batches:
        batch += 1
        now = _now(config.application.timezone)
        with connect(config.postgres) as conn:
            with conn.cursor() as cur:
                state = load_state(cur)
                stamp = now.strftime("%Y%m%d%H%M%S%f")
                inserted_order_ids: list[int] = []
                for idx in range(1, plan.orders_per_batch + 1):
                    inserted_order_ids.append(
                        insert_order(cur, rng, state, f"ORD-RT-{stamp}-{batch:04d}-{idx:03d}", now)
                    )
                changes = apply_change_events(cur, rng, state, now)
            conn.commit()

        print(
            f"batch={batch} inserted_orders={inserted_order_ids} "
            f"scd2_customer={changes.customer_id} scd2_product={changes.product_id} "
            f"scd2_shop={changes.shop_id} scd2_category={changes.category_id} "
            f"type1_product_variant={changes.product_variant_id} "
            f"advanced_order={changes.advanced_order_id} deleted_order={changes.deleted_order_id} "
            f"deleted_voucher={changes.deleted_voucher_id} inserted_voucher={changes.inserted_voucher_id} "
            f"changed_at={now.isoformat()}",
            flush=True,
        )
        if plan.max_batches is None or batch < plan.max_batches:
            time.sleep(plan.interval_seconds)


def _now(timezone_name: str) -> datetime:
    # PostgreSQL columns are TIMESTAMP WITHOUT TIME ZONE. Convert from the
    # Keep business timestamps realistic even though ingestion ordering uses event_id.
    return datetime.now(ZoneInfo(timezone_name)).replace(tzinfo=None)
