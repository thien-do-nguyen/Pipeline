from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .database import one_id
from .models import money


def seed_vouchers(cur: psycopg.Cursor[Any], now: datetime) -> list[int]:
    voucher_ids: list[int] = []
    for code, value in [("WELCOME50", 50_000), ("SALE100", 100_000), ("VIP5PCT", 5)]:
        discount_type = "percent" if code.endswith("PCT") else "fixed"
        voucher_ids.append(
            one_id(
                cur,
                """
                INSERT INTO vouchers (
                    voucher_code, voucher_name, discount_type, discount_value,
                    max_discount_amount, minimum_order_amount, scope_json,
                    starts_at, ends_at, usage_limit, is_active, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 0, %s, %s, %s, 1000, TRUE, %s, %s)
                RETURNING voucher_id
                """,
                (
                    code,
                    code,
                    discount_type,
                    money(value),
                    money(80_000) if discount_type == "percent" else None,
                    Jsonb({"type": "all"}),
                    now - timedelta(days=30),
                    now + timedelta(days=60),
                    now,
                    now,
                ),
            )
        )
    return voucher_ids
