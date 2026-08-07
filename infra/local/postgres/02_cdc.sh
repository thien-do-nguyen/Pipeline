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
    customer_app.shipments;
EOSQL
