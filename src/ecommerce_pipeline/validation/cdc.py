from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg import sql

from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.cdc_routing import CDC_DOMAIN_TABLES


@dataclass(frozen=True)
class CdcPostgresReport:
    publication_tables: int
    replica_identity_full_tables: int
    slot_active: bool
    heartbeat_age_seconds: float
    wal_retained_bytes: int


def validate_postgres_cdc(
    config: AppConfig,
    *,
    cdc_user: str,
    cdc_password: str,
    publication: str = "ecommerce_cdc_publication",
    slot: str = "ecommerce_cdc_local",
    max_heartbeat_age_seconds: int = 60,
    max_wal_retained_bytes: int = 1_073_741_824,
) -> CdcPostgresReport:
    """Verify the PostgreSQL state that Kafka Connect's REST status cannot prove."""

    expected = {table for tables in CDC_DOMAIN_TABLES.values() for table in tables}
    with psycopg.connect(config.postgres.psycopg_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT rolreplication FROM pg_roles WHERE rolname = %s", (cdc_user,))
        role = cursor.fetchone()
        if role is None or not bool(role[0]):
            raise RuntimeError(f"CDC role is missing or cannot replicate: {cdc_user}")

        cursor.execute(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = %s AND schemaname = %s",
            (publication, config.postgres.source_schema),
        )
        published = {str(row[0]) for row in cursor.fetchall()}
        if published != expected:
            raise RuntimeError(
                f"CDC publication mismatch: missing={sorted(expected - published)} extra={sorted(published - expected)}"
            )
        cursor.execute(
            """
            SELECT 1
            FROM pg_publication_tables
            WHERE pubname = %s AND schemaname = 'cdc_control' AND tablename = 'debezium_heartbeat'
            """,
            (publication,),
        )
        if cursor.fetchone() is None:
            raise RuntimeError("CDC publication is missing cdc_control.debezium_heartbeat")

        cursor.execute(
            """
            SELECT c.relname
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = ANY(%s) AND c.relreplident = 'f'
            """,
            (config.postgres.source_schema, list(expected)),
        )
        full_identity = {str(row[0]) for row in cursor.fetchall()}
        if full_identity != expected:
            raise RuntimeError(f"CDC tables missing REPLICA IDENTITY FULL: {sorted(expected - full_identity)}")

        cursor.execute(
            """
            SELECT active,
                   COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
            FROM pg_replication_slots
            WHERE slot_name = %s AND database = %s
            """,
            (slot, config.postgres.database),
        )
        slot_row = cursor.fetchone()
        if slot_row is None or not bool(slot_row[0]):
            raise RuntimeError(f"CDC replication slot is missing or inactive: {slot}")
        cursor.execute(
            """
            SELECT slot_name
            FROM pg_replication_slots
            WHERE slot_name LIKE %s AND slot_name <> %s
            ORDER BY slot_name
            """,
            (f"{slot}%", slot),
        )
        stale_sibling_slots = [str(row[0]) for row in cursor.fetchall()]
        if stale_sibling_slots:
            raise RuntimeError(f"Unexpected CDC sibling replication slots retain WAL: {stale_sibling_slots}")
        wal_retained_bytes = int(slot_row[1])
        if wal_retained_bytes > max_wal_retained_bytes:
            raise RuntimeError(
                f"CDC WAL retention exceeds threshold: retained={wal_retained_bytes}, "
                f"threshold={max_wal_retained_bytes}"
            )

        cursor.execute(
            """
            SELECT EXTRACT(EPOCH FROM (clock_timestamp() - last_seen_at))::double precision
            FROM cdc_control.debezium_heartbeat
            WHERE id = 1
            """
        )
        heartbeat_row = cursor.fetchone()
        if heartbeat_row is None:
            raise RuntimeError("Debezium heartbeat control row is missing")
        heartbeat_age_seconds = max(0.0, float(heartbeat_row[0]))
        if heartbeat_age_seconds > max_heartbeat_age_seconds:
            raise RuntimeError(
                f"Debezium heartbeat is stale: age={heartbeat_age_seconds:.1f}s, threshold={max_heartbeat_age_seconds}s"
            )

    cdc_dsn = psycopg.conninfo.make_conninfo(
        "",
        host=config.postgres.host,
        port=str(config.postgres.port),
        dbname=config.postgres.database,
        user=cdc_user,
        password=cdc_password,
        sslmode=config.postgres.sslmode or "prefer",
        connect_timeout=str(config.postgres.connect_timeout_seconds),
    )
    with psycopg.connect(cdc_dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT 1 FROM {}.{} LIMIT 1").format(
                sql.Identifier(config.postgres.source_schema),
                sql.Identifier(next(iter(sorted(expected)))),
            )
        )

    return CdcPostgresReport(
        publication_tables=len(published),
        replica_identity_full_tables=len(full_identity),
        slot_active=True,
        heartbeat_age_seconds=round(heartbeat_age_seconds, 3),
        wal_retained_bytes=wal_retained_bytes,
    )


__all__ = ["CdcPostgresReport", "validate_postgres_cdc"]
