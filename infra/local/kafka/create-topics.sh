#!/usr/bin/env bash
set -Eeuo pipefail

readonly BOOTSTRAP_SERVER="kafka:9092"
readonly KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
    local topic="$1"
    local partitions="$2"
    local cleanup_policy="$3"

    "${KAFKA_TOPICS}" \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor 1 \
        --config "cleanup.policy=${cleanup_policy}"
}

readonly CDC_TABLES=(
    app_users
    user_addresses
    shops
    categories
    products
    product_variants
    vouchers
    orders
    order_items
    order_vouchers
    payments
    shipments
)

for table_name in "${CDC_TABLES[@]}"; do
    create_topic "ecommerce.customer_app.${table_name}" 3 "delete"
done
create_topic "ecommerce.heartbeat.v1" 1 "delete"
create_topic "ecommerce.transaction.v1" 1 "delete"
create_topic "connect-configs" 1 "compact"
create_topic "connect-offsets" 3 "compact"
create_topic "connect-status" 3 "compact"
