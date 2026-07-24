from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from ecommerce_pipeline.config.models import PostgresConfig


@contextmanager
def connect(config: PostgresConfig) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(config.psycopg_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {config.source_schema}, public")
        yield conn


def one_id(cur: psycopg.Cursor[Any], query: str, params: tuple[Any, ...]) -> int:
    cur.execute(query, params)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("INSERT ... RETURNING did not return a row")
    return int(next(iter(row.values())))


def reset_source(cur: psycopg.Cursor[Any]) -> None:
    cur.execute(
        """
        TRUNCATE TABLE
            change_events, reviews, refunds, returns, shipments, payments, order_vouchers,
            order_items, orders, cart_items, shopping_carts, vouchers,
            product_variants, products, categories, shops, user_addresses, app_users
        RESTART IDENTITY CASCADE
        """
    )
