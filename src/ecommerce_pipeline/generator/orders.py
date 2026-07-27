import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast

import psycopg
from psycopg.types.json import Jsonb

from .database import one_id
from .models import RuntimeState, Variant, money
from .vouchers import rotate_delete_marker_voucher

PaymentStatus = Literal["paid", "pending", "failed"]
OrderStatus = Literal["pending_payment", "confirmed", "processing", "shipped", "delivered", "completed", "cancelled"]
ShipmentStatus = Literal["pending", "packed", "in_transit", "delivered", "cancelled"]
AddressSnapshot = dict[str, object]
OrderLine = tuple[Variant, int, Decimal, Decimal]


@dataclass(frozen=True)
class StreamChangeSummary:
    customer_id: int
    product_variant_id: int
    product_id: int
    shop_id: int
    category_id: int
    advanced_order_id: int | None
    deleted_order_id: int | None
    deleted_voucher_id: int | None
    inserted_voucher_id: int


def insert_order(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    order_number: str,
    created_at: datetime,
) -> int:
    customer_id = rng.choice(state.user_ids)
    created_at = max(created_at, state.user_created_at[customer_id])
    address_id = state.address_by_user[customer_id]
    items = rng.sample(state.variants, rng.randint(1, min(4, len(state.variants))))

    lines: list[OrderLine] = []
    subtotal = Decimal("0.00")
    tax_total = Decimal("0.00")
    line_discount_total = Decimal("0.00")
    for variant in items:
        quantity = rng.randint(1, 3)
        line_total = money(variant.unit_price * quantity)
        line_tax = money(line_total * Decimal("0.08"))
        line_discount = money(line_total * Decimal(str(rng.choice([0, 0.03, 0.05]))))
        subtotal += line_total
        tax_total += line_tax
        line_discount_total += line_discount
        lines.append((variant, quantity, line_tax, line_discount))

    shipping = money(rng.choice([0, 20_000, 35_000, 50_000]))
    order_level_discount = min(
        money(rng.choice([0, 20_000, 50_000, 80_000])),
        subtotal + shipping + tax_total - line_discount_total,
    )
    total_discount = line_discount_total + order_level_discount
    total = subtotal + shipping + tax_total - total_discount
    payment_status: PaymentStatus = rng.choice(["paid", "paid", "paid", "pending", "failed"])
    order_status = "confirmed" if payment_status == "paid" else "pending_payment"
    address_snapshot = _address_snapshot(cur, address_id)

    order_id = one_id(
        cur,
        """
        INSERT INTO orders (
            customer_id, order_number, order_status, payment_status,
            shipping_address_id, billing_address_id, shipping_address_snapshot,
            billing_address_snapshot, subtotal_amount, shipping_amount,
            tax_amount, discount_amount, currency, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s)
        RETURNING order_id
        """,
        (
            customer_id,
            order_number,
            order_status,
            payment_status,
            address_id,
            address_id,
            Jsonb(address_snapshot),
            Jsonb(address_snapshot),
            subtotal,
            shipping,
            tax_total,
            total_discount,
            created_at,
            created_at,
        ),
    )

    for variant, quantity, line_tax, line_discount in lines:
        cur.execute(
            """
            INSERT INTO order_items (
                order_id, shop_id, product_id, product_variant_id,
                product_name_snapshot, product_sku_snapshot,
                variant_name_snapshot, variant_sku_snapshot,
                variant_options_snapshot, quantity, unit_price, currency,
                tax_amount, discount_amount, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'VND', %s, %s, %s, %s)
            """,
            (
                order_id,
                variant.shop_id,
                variant.product_id,
                variant.product_variant_id,
                variant.product_name,
                variant.product_sku,
                variant.variant_name,
                variant.variant_sku,
                Jsonb({"tier": variant.variant_name.lower()}),
                quantity,
                variant.unit_price,
                line_tax,
                line_discount,
                created_at,
                created_at,
            ),
        )

    if order_level_discount > 0 and state.voucher_ids:
        cur.execute(
            """
            INSERT INTO order_vouchers (order_id, voucher_id, discount_amount, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (order_id, rng.choice(state.voucher_ids), order_level_discount, created_at, created_at),
        )

    _insert_payment(cur, rng, order_id, order_number, payment_status, total, created_at)
    _insert_shipment(cur, rng, order_id, order_number, payment_status, address_snapshot, created_at)
    return order_id


def apply_change_events(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    changed_at: datetime,
) -> StreamChangeSummary:
    customer_id = update_customer(cur, rng, state, changed_at)
    product_variant_id = update_product_variant(cur, rng, state, changed_at)
    product_id = update_product(cur, rng, state, changed_at)
    shop_id = update_shop(cur, rng, state, changed_at)
    category_id = update_category(cur, rng, changed_at)
    advanced_order_id = advance_order(cur, changed_at)
    deleted_order_id = delete_baseline_order(cur, advanced_order_id)
    deleted_voucher_id, inserted_voucher_id = rotate_delete_marker_voucher(cur, changed_at)
    return StreamChangeSummary(
        customer_id=customer_id,
        product_variant_id=product_variant_id,
        product_id=product_id,
        shop_id=shop_id,
        category_id=category_id,
        advanced_order_id=advanced_order_id,
        deleted_order_id=deleted_order_id,
        deleted_voucher_id=deleted_voucher_id,
        inserted_voucher_id=inserted_voucher_id,
    )


def update_customer(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    changed_at: datetime,
) -> int:
    user_id = rng.choice(state.user_ids)
    cur.execute(
        """
        UPDATE app_users
        SET last_login = %s,
            phone_number = %s,
            updated_at = %s
        WHERE user_id = %s
        """,
        (changed_at, f"09{rng.randint(10000000, 99999999)}", changed_at, user_id),
    )
    cur.execute(
        """
        UPDATE user_addresses
        SET street = %s,
            updated_at = %s
        WHERE address_id = %s
        """,
        (f"{rng.randint(1, 999)} Nguyen Trai", changed_at, state.address_by_user[user_id]),
    )
    return user_id


def update_product_variant(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    changed_at: datetime,
) -> int:
    variant = rng.choice(state.variants)
    new_price = money(variant.unit_price * Decimal(str(rng.choice([0.98, 1.01, 1.03]))))
    cur.execute(
        """
        UPDATE product_variants
        SET unit_price = %s,
            stock_quantity = GREATEST(stock_quantity + %s, 0),
            updated_at = %s
        WHERE product_variant_id = %s
        """,
        (new_price, rng.randint(-5, 20), changed_at, variant.product_variant_id),
    )
    return variant.product_variant_id


def update_product(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    changed_at: datetime,
) -> int:
    product_id = rng.choice(state.variants).product_id
    cur.execute(
        """
        UPDATE products
        SET is_featured = NOT is_featured,
            updated_at = %s
        WHERE product_id = %s
        """,
        (changed_at, product_id),
    )
    return product_id


def update_shop(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    state: RuntimeState,
    changed_at: datetime,
) -> int:
    shop_id = rng.choice(sorted({variant.shop_id for variant in state.variants}))
    cur.execute(
        """
        UPDATE shops
        SET shop_name = CASE
                WHEN shop_name LIKE '%% [stream]' THEN left(shop_name, length(shop_name) - length(' [stream]'))
                ELSE shop_name || ' [stream]'
            END,
            updated_at = %s
        WHERE shop_id = %s
        """,
        (changed_at, shop_id),
    )
    return shop_id


def update_category(cur: psycopg.Cursor[Any], rng: random.Random, changed_at: datetime) -> int:
    cur.execute("SELECT category_id FROM categories ORDER BY category_id")
    rows = cur.fetchall()
    if not rows:
        raise RuntimeError("No categories available for continuous generation")
    category_id = int(rng.choice(rows)["category_id"])
    cur.execute(
        """
        UPDATE categories
        SET is_active = NOT is_active,
            updated_at = %s
        WHERE category_id = %s
        """,
        (changed_at, category_id),
    )
    return category_id


def advance_order(cur: psycopg.Cursor[Any], changed_at: datetime) -> int | None:
    cur.execute(
        """
        SELECT order_id, order_status
        FROM orders
        WHERE order_status IN ('pending_payment', 'confirmed', 'processing', 'shipped', 'delivered')
        ORDER BY updated_at, order_id
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None

    order_id = int(row["order_id"])
    next_status = _next_order_status(cast(OrderStatus, row["order_status"]))
    payment_status: PaymentStatus = "paid" if next_status != "pending_payment" else "pending"

    cur.execute(
        """
        UPDATE orders
        SET order_status = %s,
            payment_status = %s,
            updated_at = %s
        WHERE order_id = %s
        """,
        (next_status, payment_status, changed_at, order_id),
    )
    cur.execute(
        """
        UPDATE payments
        SET payment_status = %s,
            paid_at = CASE WHEN %s = 'paid' THEN COALESCE(paid_at, %s) ELSE paid_at END,
            updated_at = %s
        WHERE order_id = %s
        """,
        (payment_status, payment_status, changed_at, changed_at, order_id),
    )
    cur.execute(
        """
        UPDATE shipments
        SET shipment_status = %s,
            shipped_at = CASE WHEN %s IN ('in_transit', 'delivered') THEN COALESCE(shipped_at, %s) ELSE shipped_at END,
            delivered_at = CASE WHEN %s = 'delivered' THEN COALESCE(delivered_at, %s) ELSE delivered_at END,
            updated_at = %s
        WHERE order_id = %s
        """,
        (
            _shipment_status(next_status),
            _shipment_status(next_status),
            changed_at,
            _shipment_status(next_status),
            changed_at,
            changed_at,
            order_id,
        ),
    )
    return order_id


def delete_baseline_order(cur: psycopg.Cursor[Any], exclude_order_id: int | None) -> int | None:
    cur.execute(
        """
        DELETE FROM orders
        WHERE order_id = (
            SELECT order_id
            FROM orders
            WHERE order_number ~ '^ORD-[0-9]{6}$'
              AND (%s IS NULL OR order_id <> %s)
            ORDER BY created_at, order_id
            LIMIT 1
        )
        RETURNING order_id
        """,
        (exclude_order_id, exclude_order_id),
    )
    row = cur.fetchone()
    return int(row["order_id"]) if row is not None else None


def _address_snapshot(cur: psycopg.Cursor[Any], address_id: int) -> AddressSnapshot:
    cur.execute("SELECT row_to_json(a) AS snapshot FROM user_addresses a WHERE address_id = %s", (address_id,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Missing address_id={address_id}")
    return cast(AddressSnapshot, row["snapshot"])


def _next_order_status(status: OrderStatus) -> OrderStatus:
    transitions: dict[OrderStatus, OrderStatus] = {
        "pending_payment": "confirmed",
        "confirmed": "processing",
        "processing": "shipped",
        "shipped": "delivered",
        "delivered": "completed",
        "completed": "completed",
        "cancelled": "cancelled",
    }
    return transitions[status]


def _shipment_status(order_status: OrderStatus) -> ShipmentStatus:
    mapping: dict[OrderStatus, ShipmentStatus] = {
        "pending_payment": "pending",
        "confirmed": "pending",
        "processing": "packed",
        "shipped": "in_transit",
        "delivered": "delivered",
        "completed": "delivered",
        "cancelled": "cancelled",
    }
    return mapping[order_status]


def _insert_payment(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    order_id: int,
    order_number: str,
    status: PaymentStatus,
    amount: Decimal,
    created_at: datetime,
) -> None:
    paid_at = created_at + timedelta(minutes=rng.randint(1, 60)) if status == "paid" else None
    cur.execute(
        """
        INSERT INTO payments (
            order_id, payment_method, payment_status, transaction_reference,
            amount, currency, paid_at, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, 'VND', %s, %s, %s)
        """,
        (
            order_id,
            rng.choice(["visa", "momo", "vnpay", "cod"]),
            status,
            f"TXN-{order_number}" if status == "paid" else None,
            amount,
            paid_at,
            created_at,
            created_at,
        ),
    )


def _insert_shipment(
    cur: psycopg.Cursor[Any],
    rng: random.Random,
    order_id: int,
    order_number: str,
    payment_status: PaymentStatus,
    address_snapshot: AddressSnapshot,
    created_at: datetime,
) -> None:
    shipped_at = created_at + timedelta(days=1) if payment_status == "paid" else None
    delivered_at = shipped_at + timedelta(days=rng.randint(1, 5)) if shipped_at and rng.random() > 0.25 else None
    status = "delivered" if delivered_at else ("in_transit" if shipped_at else "pending")

    cur.execute(
        """
        INSERT INTO shipments (
            order_id, shipment_status, carrier, tracking_number,
            shipping_address_snapshot, shipped_at, delivered_at, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            order_id,
            status,
            rng.choice(["GHN", "GHTK", "VNPost", "J&T"]),
            f"TRK-{order_number}" if shipped_at else None,
            Jsonb(address_snapshot),
            shipped_at,
            delivered_at,
            created_at,
            created_at,
        ),
    )
