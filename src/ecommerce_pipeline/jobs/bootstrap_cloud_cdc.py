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
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS cdc_control AUTHORIZATION {}").format(sql.Identifier(args.cdc_user))
        )
        cursor.execute(sql.SQL("ALTER SCHEMA cdc_control OWNER TO {}").format(sql.Identifier(args.cdc_user)))
        cursor.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (args.publication,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                    sql.Identifier(args.publication), sql.SQL(", ").join(tables)
                )
            )
        else:
            cursor.execute(
                sql.SQL("ALTER PUBLICATION {} SET TABLE {}").format(
                    sql.Identifier(args.publication), sql.SQL(", ").join(tables)
                )
            )
    print(
        f"[cdc-postgres] status=READY wal_level={wal_level} role={args.cdc_user} "
        f"publication={args.publication} tables={len(tables)}",
        flush=True,
    )


def main(argv: Sequence[str] | None = None) -> None:
    bootstrap(parse_args(argv))


if __name__ == "__main__":
    main()
