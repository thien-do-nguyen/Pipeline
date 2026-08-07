#!/bin/sh
set -eu

readonly CONNECT_URL="http://connect:8083"
readonly CONNECTOR_NAME="ecommerce-postgres-cdc"

curl \
    --fail-with-body \
    --silent \
    --show-error \
    --retry 30 \
    --retry-delay 2 \
    --retry-connrefused \
    --request PUT \
    --header "Content-Type: application/json" \
    --data-binary @/config/postgres-cdc.json \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"

printf '\nConnector %s is registered.\n' "${CONNECTOR_NAME}"
