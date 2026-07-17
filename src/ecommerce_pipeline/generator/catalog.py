import random
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .database import one_id
from .models import RuntimeState, Variant, money
from .users import seed_users
from .vouchers import seed_vouchers


def seed_baseline(cur: psycopg.Cursor[Any], rng: random.Random, customers: int, now: datetime) -> RuntimeState:
    categories = _seed_categories(cur)
    users, addresses, user_created_at = seed_users(cur, rng, customers, now)
    shops = _seed_shops(cur, now)
    variants = _seed_products(cur, rng, shops, categories, now)
    vouchers = seed_vouchers(cur, now)
    _seed_carts(cur, rng, users, variants, now)
    return RuntimeState(users, addresses, variants, user_created_at, vouchers)


def load_state(cur: psycopg.Cursor[Any]) -> RuntimeState:
    cur.execute("SELECT user_id, created_at FROM app_users ORDER BY user_id")
    user_rows = cur.fetchall()
    users = [int(row["user_id"]) for row in user_rows]
    user_created_at = {int(row["user_id"]): row["created_at"] for row in user_rows}

    cur.execute("SELECT user_id, address_id FROM user_addresses WHERE is_default_shipping = TRUE")
    addresses = {int(row["user_id"]): int(row["address_id"]) for row in cur.fetchall()}

    cur.execute(
        """
        SELECT p.product_id, v.product_variant_id, p.shop_id, p.product_name,
               p.product_sku, v.variant_name, v.variant_sku, v.unit_price
        FROM product_variants v
        JOIN products p ON p.product_id = v.product_id
        WHERE p.status = 'active' AND v.status = 'active'
        ORDER BY v.product_variant_id
        """
    )
    variants = [
        Variant(
            int(row["product_id"]),
            int(row["product_variant_id"]),
            int(row["shop_id"]),
            str(row["product_name"]),
            str(row["product_sku"]),
            str(row["variant_name"]),
            str(row["variant_sku"]),
            Decimal(row["unit_price"]),
        )
        for row in cur.fetchall()
    ]

    cur.execute("SELECT voucher_id FROM vouchers WHERE is_active = TRUE ORDER BY voucher_id")
    vouchers = [int(row["voucher_id"]) for row in cur.fetchall()]

    if not users or not addresses or not variants:
        raise RuntimeError("Run bootstrap first before continuous generation.")
    return RuntimeState(users, addresses, variants, user_created_at, vouchers)


def _seed_categories(cur: psycopg.Cursor[Any]) -> list[int]:
    ids: list[int] = []
    for name in ["Electronics", "Fashion", "Home", "Beauty", "Sports"]:
        ids.append(
            one_id(
                cur,
                "INSERT INTO categories (category_name, slug, description) VALUES (%s, %s, %s) RETURNING category_id",
                (name, name.lower(), f"{name} category"),
            )
        )
    return ids


def _seed_shops(cur: psycopg.Cursor[Any], now: datetime) -> list[int]:
    ids: list[int] = []
    for name in ["Blue Market", "Urban Goods", "Saigon Style", "Cloud Home"]:
        ids.append(
            one_id(
                cur,
                """
                INSERT INTO shops (shop_name, shop_slug, description, status, created_at, updated_at)
                VALUES (%s, %s, %s, 'active', %s, %s)
                RETURNING shop_id
                """,
                (name, name.lower().replace(" ", "-"), f"{name} seller", now, now),
            )
        )
    return ids


def _seed_products(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    shops: list[int],
    categories: list[int],
    now: datetime,
) -> list[Variant]:
    variants: list[Variant] = []
    product_no = 1

    for shop_id in shops:
        for _ in range(6):
            product_name = f"Product {product_no:04d}"
            product_sku = f"SKU-{product_no:04d}"
            product_id = one_id(
                cur,
                """
                INSERT INTO products (
                    shop_id, category_id, product_sku, product_slug, product_name,
                    short_description, brand, attributes_json, images_json,
                    status, is_featured, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 'Synthetic product', %s, %s, %s, 'active', %s, %s, %s)
                RETURNING product_id
                """,
                (
                    shop_id,
                    rng.choice(categories),
                    product_sku,
                    f"product-{product_no:04d}",
                    product_name,
                    rng.choice(["Aster", "Northline", "Mekong", "Nova"]),
                    Jsonb({"source": "synthetic"}),
                    Jsonb([]),
                    rng.choice([True, False]),
                    now,
                    now,
                ),
            )
            for variant_no, name in enumerate(["Standard", "Premium"], start=1):
                price = money(rng.randint(50_000, 1_500_000))
                variant_sku = f"{product_sku}-{variant_no}"
                variant_id = one_id(
                    cur,
                    """
                    INSERT INTO product_variants (
                        product_id, variant_sku, variant_name, options_json,
                        unit_price, compare_at_price, currency, stock_quantity,
                        reserved_quantity, weight_kg, images_json, status,
                        is_default, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'VND', %s, 0, %s, %s, 'active', %s, %s, %s)
                    RETURNING product_variant_id
                    """,
                    (
                        product_id,
                        variant_sku,
                        name,
                        Jsonb({"tier": name.lower()}),
                        price,
                        money(price * Decimal("1.15")),
                        rng.randint(20, 500),
                        money(rng.uniform(0.1, 3.0)),
                        Jsonb([]),
                        variant_no == 1,
                        now,
                        now,
                    ),
                )
                variants.append(
                    Variant(product_id, variant_id, shop_id, product_name, product_sku, name, variant_sku, price)
                )
            product_no += 1
    return variants


def _seed_carts(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    users: list[int],
    variants: list[Variant],
    now: datetime,
) -> None:
    for user_id in rng.sample(users, min(len(users), 20)):
        cart_id = one_id(
            cur,
            """
            INSERT INTO shopping_carts (user_id, status, created_at, updated_at)
            VALUES (%s, 'active', %s, %s)
            RETURNING cart_id
            """,
            (user_id, now, now),
        )
        for variant in rng.sample(variants, rng.randint(1, 3)):
            cur.execute(
                """
                INSERT INTO cart_items (
                    cart_id, product_variant_id, quantity, unit_price_snapshot, currency, added_at, updated_at
                )
                VALUES (%s, %s, %s, %s, 'VND', %s, %s)
                """,
                (cart_id, variant.product_variant_id, rng.randint(1, 3), variant.unit_price, now, now),
            )
