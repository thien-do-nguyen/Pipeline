import random
from datetime import datetime, timedelta
from typing import Any

import psycopg

from .database import many_returning
from .models import AddressSnapshot


def seed_users(
    cur: psycopg.Cursor[Any], rng: random.Random, customers: int, now: datetime
) -> tuple[list[int], dict[int, int], dict[int, AddressSnapshot], dict[int, datetime]]:
    cities = ["Ho Chi Minh City", "Ha Noi", "Da Nang", "Can Tho", "Hue"]
    user_params: list[tuple[Any, ...]] = []
    address_values: list[tuple[str, str, str, str, str, str, datetime]] = []
    created_times: list[datetime] = []

    for idx in range(1, customers + 1):
        created_at = now - timedelta(days=rng.randint(30, 365))
        phone = f"090{idx:07d}"[:10]
        user_params.append(
            (
                f"user{idx:04d}",
                f"user{idx:04d}@example.com",
                f"First{idx}",
                f"Last{idx}",
                phone,
                created_at,
                now,
                now - timedelta(days=rng.randint(0, 10)),
            )
        )
        address_values.append(
            (
                f"First{idx} Last{idx}",
                phone,
                f"{idx} Nguyen Trai",
                f"Ward {rng.randint(1, 12)}",
                f"District {rng.randint(1, 12)}",
                rng.choice(cities),
                created_at,
            )
        )
        created_times.append(created_at)

    user_rows = many_returning(
        cur,
        """
        INSERT INTO app_users (
            username, email, password_hash, first_name, last_name,
            phone_number, status, created_at, updated_at, last_login
        )
        VALUES (%s, %s, 'synthetic_hash', %s, %s, %s, 'active', %s, %s, %s)
        RETURNING user_id
        """,
        user_params,
    )
    users = [int(row["user_id"]) for row in user_rows]
    if len(users) != customers:
        raise RuntimeError(f"Expected {customers} inserted users, received {len(users)}")

    address_params = [(user_id, *values, now) for user_id, values in zip(users, address_values, strict=True)]
    address_rows = many_returning(
        cur,
        """
        INSERT INTO user_addresses (
            user_id, address_type, recipient_name, phone_number, street,
            ward, district, city, country, is_default_shipping,
            is_default_billing, created_at, updated_at
        )
        VALUES (%s, 'shipping', %s, %s, %s, %s, %s, %s, 'Vietnam', TRUE, TRUE, %s, %s)
        RETURNING user_id, address_id, to_jsonb(user_addresses) AS snapshot
        """,
        address_params,
    )
    if len(address_rows) != customers:
        raise RuntimeError(f"Expected {customers} inserted addresses, received {len(address_rows)}")

    addresses: dict[int, int] = {}
    snapshots: dict[int, AddressSnapshot] = {}
    created_by_user: dict[int, datetime] = {}
    for user_id, created_at, row in zip(users, created_times, address_rows, strict=True):
        addresses[user_id] = int(row["address_id"])
        snapshots[user_id] = dict(row["snapshot"])
        created_by_user[user_id] = created_at
    return users, addresses, snapshots, created_by_user
