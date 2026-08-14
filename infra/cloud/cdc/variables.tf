variable "subscription_id" {
  description = "Azure subscription that owns the existing CDC resource group."
  type        = string
}

variable "resource_group_name" {
  description = "Existing resource group dedicated to ephemeral CDC resources."
  type        = string
  default     = "rg-tk1-student-cdc-dev"
}

variable "name_prefix" {
  description = "Globally unique-friendly prefix for Event Hubs and ACI names."
  type        = string
  default     = "tk1-ecommerce-cdc-dev"
}

variable "debezium_image" {
  description = "Pinned Debezium Server image."
  type        = string
  default     = "quay.io/debezium/server:3.6.0.Final"
}

variable "postgres_host" {
  type = string
}

variable "postgres_port" {
  type    = number
  default = 5432
}

variable "postgres_database" {
  type = string
}

variable "postgres_cdc_user" {
  type    = string
  default = "ecommerce_cdc"
}

variable "postgres_cdc_password" {
  description = "Password of the pre-created PostgreSQL logical replication role."
  type        = string
  sensitive   = true
}

variable "tags" {
  type = map(string)
  default = {
    environment = "dev"
    project     = "ecommerce-lakehouse"
    workload    = "ephemeral-cdc"
    managed-by  = "terraform"
  }
}
