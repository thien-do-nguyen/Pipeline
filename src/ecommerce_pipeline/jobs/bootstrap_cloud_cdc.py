from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

import psycopg
from dotenv import load_dotenv
from psycopg import sql

from ecommerce_pipeline.contracts.cdc_routing import CDC_DOMAIN_TABLES


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Idempotently bootstrap Azure PostgreSQL logical CDC objects")
    parser.add_argument("--env-file", default=".env.cloud")
    parser.add_argument("--cdc-user", default="ecommerce_cdc")
    parser.add_argument("--publication", default="ecommerce_cdc_publication")
    parser.add_argument("--slot", default="ecommerce_cdc_cloud")
    parser.add_argument(
        "--reset-runtime-state",
        action="store_true",
        help="Drop an inactive replication slot and Debezium offsets after ephemeral CDC infrastructure is destroyed.",
    )
    return parser.parse_args(argv)


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required in the cloud environment")
    return value


def bootstrap(args: argparse.Namespace) -> None:
    load_dotenv(args.env_file, override=False)
    cdc_password = _required("CDC_POSTGRES_PASSWORD")
    host = _required("POSTGRES_HOST")
    port = int(_required("POSTGRES_PORT"))
    database = _required("POSTGRES_DB")
    admin_user = _required("POSTGRES_USER")
    admin_password = _required("POSTGRES_PASSWORD")
    table_names = [table for tables in CDC_DOMAIN_TABLES.values() for table in tables]
    tables = [sql.Identifier("customer_app", table) for table in table_names]
    publication_tables = [*tables, sql.Identifier("cdc_control", "debezium_heartbeat")]
    with (
        psycopg.connect(
            host=host,
            port=port,
            dbname=database,
            user=admin_user,
            password=admin_password,
            sslmode="require",
            connect_timeout=15,
            autocommit=True,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SHOW wal_level")
        wal_level_row = cursor.fetchone()
        if wal_level_row is None:
            raise RuntimeError("PostgreSQL did not return wal_level")
        wal_level = str(wal_level_row[0])
        if wal_level != "logical":
            raise RuntimeError(
                "PostgreSQL wal_level is not logical. Set the static server parameter "
                "wal_level=logical on Azure Database for PostgreSQL Flexible Server, save it, "
                "and restart the server."
            )
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (args.cdc_user,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
                    sql.Identifier(args.cdc_user),
                    sql.Literal(cdc_password),
                )
            )
        else:
            cursor.execute(
                sql.SQL("ALTER ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
                    sql.Identifier(args.cdc_user),
                    sql.Literal(cdc_password),
                )
            )
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database), sql.Identifier(args.cdc_user)
            )
        )
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA customer_app TO {}").format(sql.Identifier(args.cdc_user)))
        cursor.execute(
            sql.SQL("GRANT SELECT ON TABLE {} TO {}").format(sql.SQL(", ").join(tables), sql.Identifier(args.cdc_user))
        )
        for table in tables:
            cursor.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(table))
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS cdc_control AUTHORIZATION {}").format(sql.Identifier(args.cdc_user))
        )
        cursor.execute(sql.SQL("ALTER SCHEMA cdc_control OWNER TO {}").format(sql.Identifier(args.cdc_user)))
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_control.debezium_heartbeat (
                id smallint PRIMARY KEY CHECK (id = 1),
                last_seen_at timestamptz NOT NULL
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO cdc_control.debezium_heartbeat (id, last_seen_at)
            VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
            ON CONFLICT (id) DO NOTHING
            """
        )
        cursor.execute(
            sql.SQL("ALTER TABLE cdc_control.debezium_heartbeat OWNER TO {}").format(sql.Identifier(args.cdc_user))
        )
        cursor.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (args.publication,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                    sql.Identifier(args.publication), sql.SQL(", ").join(publication_tables)
                )
            )
        else:
            cursor.execute(
                sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(
                    sql.Identifier(args.publication), sql.SQL(", ").join(publication_tables)
                )
            )
        cursor.execute(
            """
            SELECT count(*)
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'customer_app' AND c.relname = ANY(%s) AND c.relreplident = 'f'
            """,
            (table_names,),
        )
        identity_count_row = cursor.fetchone()
        identity_count = 0 if identity_count_row is None else int(identity_count_row[0])
        if identity_count != len(table_names):
            raise RuntimeError(
                f"REPLICA IDENTITY FULL verification failed: expected={len(table_names)} actual={identity_count}"
            )
    print(
        f"[cdc-postgres] status=READY wal_level={wal_level} role={args.cdc_user} "
        f"publication={args.publication} tables={len(tables)} replica_identity_full={identity_count}",
        flush=True,
    )


def reset_runtime_state(args: argparse.Namespace) -> None:
    """Remove resumable CDC state only after the external Debezium runtime is stopped."""

    load_dotenv(args.env_file, override=False)
    with (
        psycopg.connect(
            host=_required("POSTGRES_HOST"),
            port=int(_required("POSTGRES_PORT")),
            dbname=_required("POSTGRES_DB"),
            user=args.cdc_user,
            password=_required("CDC_POSTGRES_PASSWORD"),
            sslmode="require",
            connect_timeout=15,
            autocommit=True,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (args.slot,))
        slot = cursor.fetchone()
        if slot is not None and bool(slot[0]):
            raise RuntimeError(f"Refusing to reset active replication slot {args.slot!r}; stop/destroy Debezium first")
        if slot is not None:
            cursor.execute("SELECT pg_drop_replication_slot(%s)", (args.slot,))
        cursor.execute(
            """
            DROP TABLE IF EXISTS
                cdc_control.debezium_offset_storage,
                cdc_control.debezium_offset_storage_a,
                cdc_control.debezium_offset_storage_b
            """
        )
        cursor.execute("SELECT to_regclass('cdc_control.debezium_heartbeat')")
        heartbeat_table = cursor.fetchone()
        if heartbeat_table is not None and heartbeat_table[0] is not None:
            cursor.execute(
                """
                UPDATE cdc_control.debezium_heartbeat
                SET last_seen_at = TIMESTAMPTZ '1970-01-01 00:00:00+00'
                WHERE id = 1
                """
            )
    print(
        f"[cdc-postgres] status=RUNTIME_STATE_RESET slot={args.slot} source_data=PRESERVED",
        flush=True,
    )


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.reset_runtime_state:
        reset_runtime_state(args)
    else:
        bootstrap(args)


if __name__ == "__main__":
    main()
