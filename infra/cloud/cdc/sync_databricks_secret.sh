#!/usr/bin/env bash
set -Eeuo pipefail

terraform_dir="${1:?terraform directory is required}"
secret_scope="${2:?Databricks secret scope is required}"
databricks_profile="${3:?Databricks profile is required}"

connection_string="$(terraform -chdir="${terraform_dir}" output -raw databricks_listen_connection_string)"
secret_key="event-hubs-listen-connection-string"
printf '%s' "${connection_string}" |
  databricks secrets put-secret "${secret_scope}" "${secret_key}" \
    --profile "${databricks_profile}"
printf '[cdc-secret] status=SYNCED scope=%s key=%s\n' "${secret_scope}" "${secret_key}"
