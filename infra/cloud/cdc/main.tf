locals {
  # Standard supports 10 hubs. Domain routing keeps table identity in
  # Debezium's source.table while reducing 12 physical-table topics to six.
  domain_tables = {
    customer  = ["app_users", "user_addresses"]
    catalog   = ["shops", "categories", "products", "product_variants"]
    promotion = ["vouchers"]
    sales     = ["orders", "order_items", "order_vouchers"]
    payment   = ["payments"]
    shipping  = ["shipments"]
  }
  data_event_hubs = {
    for domain in keys(local.domain_tables) : domain => "ecommerce.domain.${domain}"
  }
  event_hubs = merge(local.data_event_hubs, {
    heartbeat   = "ecommerce.heartbeat.v1"
    transaction = "ecommerce.transaction.v1"
  })
  source_tables = sort(flatten(values(local.domain_tables)))
  table_include_list = join(",", [
    for table in local.source_tables : "customer_app.${table}"
  ])
  kafka_jaas = format(
    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";",
    azurerm_eventhub_namespace_authorization_rule.debezium_send.primary_connection_string,
  )
}

data "azurerm_resource_group" "cdc" {
  name = var.resource_group_name
}

# Re-created after destroy/apply. The suffix is also the Spark source identity,
# preventing offsets from an old namespace being reused against a fresh one.
resource "random_id" "source_epoch" {
  byte_length = 3
}

resource "azurerm_eventhub_namespace" "cdc" {
  name                          = "${var.name_prefix}-${random_id.source_epoch.hex}"
  location                      = data.azurerm_resource_group.cdc.location
  resource_group_name           = data.azurerm_resource_group.cdc.name
  sku                           = "Standard"
  capacity                      = 1
  auto_inflate_enabled          = false
  public_network_access_enabled = true
  local_authentication_enabled  = true
  minimum_tls_version           = "1.2"
  tags                          = var.tags
}

resource "azurerm_eventhub" "topics" {
  for_each = local.event_hubs

  name              = each.value
  namespace_id      = azurerm_eventhub_namespace.cdc.id
  partition_count   = 1
  message_retention = 1
}

resource "azurerm_eventhub_namespace_authorization_rule" "debezium_send" {
  name                = "debezium-send"
  namespace_name      = azurerm_eventhub_namespace.cdc.name
  resource_group_name = data.azurerm_resource_group.cdc.name
  listen              = false
  send                = true
  manage              = false
}

resource "azurerm_eventhub_namespace_authorization_rule" "databricks_listen" {
  name                = "databricks-listen"
  namespace_name      = azurerm_eventhub_namespace.cdc.name
  resource_group_name = data.azurerm_resource_group.cdc.name
  listen              = true
  send                = false
  manage              = false
}

resource "azurerm_container_group" "debezium" {
  name                = "aci-debezium-${random_id.source_epoch.hex}"
  location            = data.azurerm_resource_group.cdc.location
  resource_group_name = data.azurerm_resource_group.cdc.name
  ip_address_type     = "None"
  os_type             = "Linux"
  restart_policy      = "Always"
  tags                = var.tags

  container {
    name   = "debezium-server"
    image  = var.debezium_image
    cpu    = "1.0"
    memory = "1.5"

    environment_variables = {
      JAVA_OPTS_APPEND                                          = "-Xms256m -Xmx768m"
      DEBEZIUM_SINK_TYPE                                        = "kafka"
      DEBEZIUM_SINK_KAFKA_PRODUCER_BOOTSTRAP_SERVERS            = "${azurerm_eventhub_namespace.cdc.name}.servicebus.windows.net:9093"
      DEBEZIUM_SINK_KAFKA_PRODUCER_SECURITY_PROTOCOL            = "SASL_SSL"
      DEBEZIUM_SINK_KAFKA_PRODUCER_SASL_MECHANISM               = "PLAIN"
      DEBEZIUM_SINK_KAFKA_PRODUCER_KEY_SERIALIZER               = "org.apache.kafka.common.serialization.StringSerializer"
      DEBEZIUM_SINK_KAFKA_PRODUCER_VALUE_SERIALIZER             = "org.apache.kafka.common.serialization.StringSerializer"
      DEBEZIUM_SINK_KAFKA_PRODUCER_ACKS                         = "all"
      DEBEZIUM_SINK_KAFKA_PRODUCER_ENABLE_IDEMPOTENCE           = "false"
      DEBEZIUM_SINK_KAFKA_WAIT_MESSAGE_DELIVERY_TIMEOUT_MS      = "60000"
      DEBEZIUM_FORMAT_KEY_SCHEMAS_ENABLE                        = "false"
      DEBEZIUM_FORMAT_VALUE_SCHEMAS_ENABLE                      = "false"
      DEBEZIUM_SOURCE_CONNECTOR_CLASS                           = "io.debezium.connector.postgresql.PostgresConnector"
      DEBEZIUM_SOURCE_DATABASE_HOSTNAME                         = var.postgres_host
      DEBEZIUM_SOURCE_DATABASE_PORT                             = tostring(var.postgres_port)
      DEBEZIUM_SOURCE_DATABASE_USER                             = var.postgres_cdc_user
      DEBEZIUM_SOURCE_DATABASE_DBNAME                           = var.postgres_database
      DEBEZIUM_SOURCE_DATABASE_SSLMODE                          = "require"
      DEBEZIUM_SOURCE_TOPIC_PREFIX                              = "ecommerce"
      DEBEZIUM_SOURCE_PLUGIN_NAME                               = "pgoutput"
      DEBEZIUM_SOURCE_SLOT_NAME                                 = "ecommerce_cdc_cloud"
      DEBEZIUM_SOURCE_SLOT_DROP_ON_STOP                         = "false"
      DEBEZIUM_SOURCE_PUBLICATION_NAME                          = "ecommerce_cdc_publication"
      DEBEZIUM_SOURCE_PUBLICATION_AUTOCREATE_MODE               = "disabled"
      DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST                       = "customer_app"
      DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST                        = local.table_include_list
      DEBEZIUM_SOURCE_COLUMN_EXCLUDE_LIST                       = "customer_app.app_users.password_hash"
      DEBEZIUM_SOURCE_SNAPSHOT_MODE                             = "when_needed"
      DEBEZIUM_SOURCE_PROVIDE_TRANSACTION_METADATA              = "true"
      DEBEZIUM_SOURCE_HEARTBEAT_INTERVAL_MS                     = "10000"
      DEBEZIUM_SOURCE_TOPIC_HEARTBEAT_NAME                      = "ecommerce.heartbeat.v1"
      DEBEZIUM_SOURCE_TOPIC_TRANSACTION                         = "transaction.v1"
      DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE                      = "false"
      DEBEZIUM_SOURCE_DECIMAL_HANDLING_MODE                     = "string"
      DEBEZIUM_SOURCE_BINARY_HANDLING_MODE                      = "base64"
      DEBEZIUM_SOURCE_OFFSET_STORAGE                            = "io.debezium.storage.jdbc.offset.JdbcOffsetBackingStore"
      DEBEZIUM_SOURCE_OFFSET_STORAGE_JDBC_URL                   = "jdbc:postgresql://${var.postgres_host}:${var.postgres_port}/${var.postgres_database}?sslmode=require&currentSchema=cdc_control"
      DEBEZIUM_SOURCE_OFFSET_STORAGE_JDBC_USER                  = var.postgres_cdc_user
      DEBEZIUM_SOURCE_OFFSET_STORAGE_JDBC_OFFSET_TABLE_NAME     = "debezium_offset_storage"
      DEBEZIUM_SOURCE_OFFSET_FLUSH_INTERVAL_MS                  = "1000"
      # MicroProfile normalizes environment variable names to lower case. The
      # symbolic names must match or Debezium cannot resolve their `.type`.
      DEBEZIUM_TRANSFORMS                                       = "routecustomer,routecatalog,routepromotion,routesales,routepayment,routeshipping"
      DEBEZIUM_TRANSFORMS_ROUTECUSTOMER_TYPE                    = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTECUSTOMER_TOPIC_REGEX             = "ecommerce\\.customer_app\\.(app_users|user_addresses)"
      DEBEZIUM_TRANSFORMS_ROUTECUSTOMER_TOPIC_REPLACEMENT       = local.data_event_hubs.customer
      DEBEZIUM_TRANSFORMS_ROUTECUSTOMER_KEY_ENFORCE_UNIQUENESS  = "true"
      DEBEZIUM_TRANSFORMS_ROUTECATALOG_TYPE                     = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTECATALOG_TOPIC_REGEX              = "ecommerce\\.customer_app\\.(shops|categories|products|product_variants)"
      DEBEZIUM_TRANSFORMS_ROUTECATALOG_TOPIC_REPLACEMENT        = local.data_event_hubs.catalog
      DEBEZIUM_TRANSFORMS_ROUTECATALOG_KEY_ENFORCE_UNIQUENESS   = "true"
      DEBEZIUM_TRANSFORMS_ROUTEPROMOTION_TYPE                   = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTEPROMOTION_TOPIC_REGEX            = "ecommerce\\.customer_app\\.vouchers"
      DEBEZIUM_TRANSFORMS_ROUTEPROMOTION_TOPIC_REPLACEMENT      = local.data_event_hubs.promotion
      DEBEZIUM_TRANSFORMS_ROUTEPROMOTION_KEY_ENFORCE_UNIQUENESS = "true"
      DEBEZIUM_TRANSFORMS_ROUTESALES_TYPE                       = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTESALES_TOPIC_REGEX                = "ecommerce\\.customer_app\\.(orders|order_items|order_vouchers)"
      DEBEZIUM_TRANSFORMS_ROUTESALES_TOPIC_REPLACEMENT          = local.data_event_hubs.sales
      DEBEZIUM_TRANSFORMS_ROUTESALES_KEY_ENFORCE_UNIQUENESS     = "true"
      DEBEZIUM_TRANSFORMS_ROUTEPAYMENT_TYPE                     = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTEPAYMENT_TOPIC_REGEX              = "ecommerce\\.customer_app\\.payments"
      DEBEZIUM_TRANSFORMS_ROUTEPAYMENT_TOPIC_REPLACEMENT        = local.data_event_hubs.payment
      DEBEZIUM_TRANSFORMS_ROUTEPAYMENT_KEY_ENFORCE_UNIQUENESS   = "true"
      DEBEZIUM_TRANSFORMS_ROUTESHIPPING_TYPE                    = "io.debezium.transforms.ByLogicalTableRouter"
      DEBEZIUM_TRANSFORMS_ROUTESHIPPING_TOPIC_REGEX             = "ecommerce\\.customer_app\\.shipments"
      DEBEZIUM_TRANSFORMS_ROUTESHIPPING_TOPIC_REPLACEMENT       = local.data_event_hubs.shipping
      DEBEZIUM_TRANSFORMS_ROUTESHIPPING_KEY_ENFORCE_UNIQUENESS  = "true"
      QUARKUS_LOG_CONSOLE_JSON_ENABLED                          = "true"
    }

    secure_environment_variables = {
      DEBEZIUM_SOURCE_DATABASE_PASSWORD             = var.postgres_cdc_password
      DEBEZIUM_SOURCE_OFFSET_STORAGE_JDBC_PASSWORD  = var.postgres_cdc_password
      DEBEZIUM_SINK_KAFKA_PRODUCER_SASL_JAAS_CONFIG = local.kafka_jaas
    }
  }

  depends_on = [azurerm_eventhub.topics]
}
