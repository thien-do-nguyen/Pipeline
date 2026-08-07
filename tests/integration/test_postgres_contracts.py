from __future__ import annotations

import os

import pytest

from ecommerce_pipeline.config.loader import load_config
from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES
from ecommerce_pipeline.generator.database import connect


@pytest.mark.integration
def test_postgres_contains_every_declared_batch_source() -> None:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 with PostgreSQL running")
    config = load_config("local")

    with connect(config.postgres) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (config.postgres.source_schema,),
        )
        actual_tables = {str(row["table_name"]) for row in cursor.fetchall()}

        for table_name, contract in BRONZE_TABLES.items():
            assert table_name in actual_tables
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (config.postgres.source_schema, table_name),
            )
            columns = {str(row["column_name"]) for row in cursor.fetchall()}
            assert set(contract.primary_keys) <= columns

        cursor.execute(
            """
            SELECT event_object_table
            FROM information_schema.triggers
            WHERE trigger_schema = %s AND trigger_name LIKE 'capture_%%_change'
            """,
            (config.postgres.source_schema,),
        )
        triggered_tables = {str(row["event_object_table"]) for row in cursor.fetchall()}
        assert set(BRONZE_TABLES) == triggered_tables


@pytest.mark.integration
def test_change_event_log_captures_insert_update_and_delete_without_password_hash() -> None:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 with PostgreSQL running")
    config = load_config("local")

    with connect(config.postgres) as connection, connection.cursor() as cursor:
        cursor.execute("TRUNCATE change_events RESTART IDENTITY")
        cursor.execute(
            """
            INSERT INTO app_users (username, email, password_hash)
            VALUES ('event_test', 'event-test@example.com', 'must-not-leak')
            RETURNING user_id
            """
        )
        user_id = int(cursor.fetchone()["user_id"])
        cursor.execute("UPDATE app_users SET phone_number = '0900000000' WHERE user_id = %s", (user_id,))
        cursor.execute("DELETE FROM app_users WHERE user_id = %s", (user_id,))
        cursor.execute(
            """
            SELECT operation, primary_key, row_data
            FROM change_events
            WHERE source_table = 'app_users'
            ORDER BY event_id
            """
        )
        events = cursor.fetchall()

    assert [event["operation"] for event in events] == ["INSERT", "UPDATE", "DELETE"]
    assert all(event["primary_key"] == {"user_id": user_id} for event in events)
    assert all("password_hash" not in event["row_data"] for event in events)


@pytest.mark.integration
def test_debezium_role_and_publication_match_the_cdc_contract() -> None:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 with PostgreSQL running")
    config = load_config("local")

    with connect(config.postgres) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT rolreplication FROM pg_roles WHERE rolname = 'ecommerce_cdc'")
        role = cursor.fetchone()
        cursor.execute(
            """
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = 'ecommerce_cdc_publication'
            """
        )
        published = {(str(row["schemaname"]), str(row["tablename"])) for row in cursor.fetchall()}

    assert role is not None and role["rolreplication"] is True
    assert published == {(config.postgres.source_schema, table) for table in BRONZE_TABLES}
