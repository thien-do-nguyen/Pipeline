from __future__ import annotations

import psycopg

from ecommerce_pipeline.config.models import AppConfig

CHANGE_EVENT_COLUMNS = {"event_id", "source_table", "operation", "occurred_at", "row_data"}


def check_postgres_source(config: AppConfig) -> dict[str, object]:
    """Verify connectivity and the trigger-CDC outbox contract without Spark."""

    relation = f"{config.postgres.source_schema}.change_events"
    with (
        psycopg.connect(
            config.postgres.psycopg_dsn,
            connect_timeout=config.postgres.connect_timeout_seconds,
            autocommit=True,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT current_database(), current_user, "
            "to_regclass(%s) IS NOT NULL, "
            "has_schema_privilege(current_user, %s, 'USAGE'), "
            "has_table_privilege(current_user, %s, 'SELECT')",
            (relation, config.postgres.source_schema, relation),
        )
        database, user, table_exists, schema_usage, table_select = cursor.fetchone() or (None,) * 5
        if not table_exists:
            raise RuntimeError(f"PostgreSQL CDC outbox does not exist: {relation}")
        if not schema_usage or not table_select:
            raise PermissionError(f"PostgreSQL user lacks USAGE/SELECT for CDC outbox: {relation}")

        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'change_events'",
            (config.postgres.source_schema,),
        )
        columns = {str(row[0]) for row in cursor.fetchall()}

    missing = sorted(CHANGE_EVENT_COLUMNS - columns)
    if missing:
        raise RuntimeError(f"PostgreSQL CDC outbox is missing columns: {missing}")
    return {
        "database": str(database),
        "user": str(user),
        "outbox": relation,
        "required_columns": sorted(CHANGE_EVENT_COLUMNS),
    }


__all__ = ["check_postgres_source"]
