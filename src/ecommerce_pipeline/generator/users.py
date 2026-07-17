import random
from datetime import datetime, timedelta
from typing import Any

import psycopg

from .database import one_id


def seed_users(
    cur: psycopg.Cursor[Any], rng: random.Random, customers: int, now: datetime
) -> tuple[list[int], dict[int, int], dict[int, datetime]]:
    cities = ["Ho Chi Minh City", "Ha Noi", "Da Nang", "Can Tho", "Hue"]
    users: list[int] = []
    addresses: dict[int, int] = {}
    created_by_user: dict[int, datetime] = {}

    for idx in range(1, customers + 1):
        created_at = now - timedelta(days=rng.randint(30, 365))
        user_id = one_id(
            cur,
            """
            INSERT INTO app_users (
                username, email, password_hash, first_name, last_name,
                phone_number, status, created_at, updated_at, last_login
            )
            VALUES (%s, %s, 'synthetic_hash', %s, %s, %s, 'active', %s, %s, %s)
            RETURNING user_id
            """,
            (
                f"user{idx:04d}",
                f"user{idx:04d}@example.com",
                f"First{idx}",
                f"Last{idx}",
                f"090{idx:07d}"[:10],
                created_at,
                now,
                now - timedelta(days=rng.randint(0, 10)),
            ),
        )
        address_id = one_id(
            cur,
            """
            INSERT INTO user_addresses (
                user_id, address_type, recipient_name, phone_number, street,
                ward, district, city, country, is_default_shipping,
                is_default_billing, created_at, updated_at
            )
            VALUES (%s, 'shipping', %s, %s, %s, %s, %s, %s, 'Vietnam', TRUE, TRUE, %s, %s)
            RETURNING address_id
            """,
            (
                user_id,
                f"First{idx} Last{idx}",
                f"090{idx:07d}"[:10],
                f"{idx} Nguyen Trai",
                f"Ward {rng.randint(1, 12)}",
                f"District {rng.randint(1, 12)}",
                rng.choice(cities),
                created_at,
                now,
            ),
        )
        users.append(user_id)
        addresses[user_id] = address_id
        created_by_user[user_id] = created_at
    return users, addresses, created_by_user
