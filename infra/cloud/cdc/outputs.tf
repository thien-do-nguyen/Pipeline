output "event_hubs_namespace" {
  description = "Kafka bootstrap namespace and source epoch used by the Databricks CDC job."
  value       = azurerm_eventhub_namespace.cdc.name
}

output "event_hubs_bootstrap_server" {
  value = "${azurerm_eventhub_namespace.cdc.name}.servicebus.windows.net:9093"
}

output "databricks_listen_connection_string" {
  description = "Store this value in the Databricks secret scope; never commit it."
  value       = azurerm_eventhub_namespace_authorization_rule.databricks_listen.primary_connection_string
  sensitive   = true
}

output "debezium_container_group" {
  value = azurerm_container_group.debezium.name
}

output "event_hub_topics" {
  value = sort(values(local.event_hubs))
}
