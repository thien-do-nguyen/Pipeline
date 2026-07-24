from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .database import one_id
from .models import money

DELETE_MARKER_PREFIX = "CDC_DELETE_"


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
    _insert_delete_marker_voucher(cur, now, "BOOTSTRAP")
    return voucher_ids


def rotate_delete_marker_voucher(cur: psycopg.Cursor[Any], changed_at: datetime) -> None:
    cur.execute(
        """
        DELETE FROM vouchers
        WHERE voucher_id = (
            SELECT v.voucher_id
            FROM vouchers v
            LEFT JOIN order_vouchers ov ON ov.voucher_id = v.voucher_id
            WHERE v.voucher_code LIKE %s AND ov.voucher_id IS NULL
            ORDER BY v.updated_at, v.voucher_id
            LIMIT 1
        )
        """,
        (f"{DELETE_MARKER_PREFIX}%",),
    )
    _insert_delete_marker_voucher(cur, changed_at, changed_at.strftime("%Y%m%d%H%M%S%f"))


def _insert_delete_marker_voucher(cur: psycopg.Cursor[Any], changed_at: datetime, suffix: str) -> None:
    code = f"{DELETE_MARKER_PREFIX}{suffix}"
    cur.execute(
        """
        INSERT INTO vouchers (
            voucher_code, voucher_name, discount_type, discount_value,
            max_discount_amount, minimum_order_amount, scope_json,
            starts_at, ends_at, usage_limit, is_active, created_at, updated_at
        )
        VALUES (%s, %s, 'fixed', %s, NULL, 0, %s, %s, %s, 1, FALSE, %s, %s)
        """,
        (
            code,
            code,
            money(1_000),
            Jsonb({"type": "delete-marker"}),
            changed_at - timedelta(days=1),
            changed_at + timedelta(days=1),
            changed_at,
            changed_at,
        ),
    )
