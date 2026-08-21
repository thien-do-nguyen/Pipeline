#!/usr/bin/env bash
set -Eeuo pipefail

: "${CDC_POSTGRES_PASSWORD:?CDC_POSTGRES_PASSWORD is required}"

psql --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" --set ON_ERROR_STOP=1 <<'EOSQL'
\getenv cdc_password CDC_POSTGRES_PASSWORD

SELECT format(
    'CREATE ROLE ecommerce_cdc WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_password'
)
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ecommerce_cdc')
\gexec

ALTER ROLE ecommerce_cdc WITH LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO ecommerce_cdc;
GRANT USAGE ON SCHEMA customer_app TO ecommerce_cdc;
GRANT SELECT ON TABLE
    customer_app.app_users,
    customer_app.user_addresses,
    customer_app.shops,
    customer_app.categories,
    customer_app.products,
    customer_app.product_variants,
    customer_app.vouchers,
    customer_app.orders,
    customer_app.order_items,
    customer_app.order_vouchers,
    customer_app.payments,
    customer_app.shipments
TO ecommerce_cdc;

ALTER DEFAULT PRIVILEGES IN SCHEMA customer_app
    GRANT SELECT ON TABLES TO ecommerce_cdc;

CREATE SCHEMA IF NOT EXISTS cdc_control AUTHORIZATION ecommerce_cdc;
CREATE TABLE IF NOT EXISTS cdc_control.debezium_heartbeat (
    id smallint PRIMARY KEY CHECK (id = 1),
    last_seen_at timestamptz NOT NULL
);
INSERT INTO cdc_control.debezium_heartbeat (id, last_seen_at)
VALUES (1, TIMESTAMPTZ '1970-01-01 00:00:00+00')
ON CONFLICT (id) DO NOTHING;
ALTER TABLE cdc_control.debezium_heartbeat OWNER TO ecommerce_cdc;

ALTER TABLE customer_app.app_users REPLICA IDENTITY FULL;
ALTER TABLE customer_app.user_addresses REPLICA IDENTITY FULL;
ALTER TABLE customer_app.shops REPLICA IDENTITY FULL;
ALTER TABLE customer_app.categories REPLICA IDENTITY FULL;
ALTER TABLE customer_app.products REPLICA IDENTITY FULL;
ALTER TABLE customer_app.product_variants REPLICA IDENTITY FULL;
ALTER TABLE customer_app.vouchers REPLICA IDENTITY FULL;
ALTER TABLE customer_app.orders REPLICA IDENTITY FULL;
ALTER TABLE customer_app.order_items REPLICA IDENTITY FULL;
ALTER TABLE customer_app.order_vouchers REPLICA IDENTITY FULL;
ALTER TABLE customer_app.payments REPLICA IDENTITY FULL;
ALTER TABLE customer_app.shipments REPLICA IDENTITY FULL;

SELECT 'CREATE PUBLICATION ecommerce_cdc_publication'
WHERE NOT EXISTS (
    SELECT FROM pg_publication WHERE pubname = 'ecommerce_cdc_publication'
)
\gexec

ALTER PUBLICATION ecommerce_cdc_publication SET TABLE
    customer_app.app_users,
    customer_app.user_addresses,
    customer_app.shops,
    customer_app.categories,
    customer_app.products,
    customer_app.product_variants,
    customer_app.vouchers,
    customer_app.orders,
    customer_app.order_items,
    customer_app.order_vouchers,
    customer_app.payments,
    customer_app.shipments,
    cdc_control.debezium_heartbeat;
EOSQL
