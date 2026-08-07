from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_pipeline.contracts.bronze_tables import BRONZE_SCHEMA_VERSION, BRONZE_TABLES
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES

_BRONZE_ONLY_COLUMNS: dict[str, tuple[str, ...]] = {
    "app_users": (),
    "user_addresses": ("is_default_shipping", "is_default_billing"),
    "shops": ("description", "logo_url"),
    "categories": ("description",),
    "products": ("short_description", "description"),
    "product_variants": ("dimensions_json",),
    "vouchers": ("description", "usage_limit", "used_count"),
    "orders": ("customer_note",),
    "order_items": (),
    "order_vouchers": (),
    "payments": ("transaction_reference",),
    "shipments": ("shipping_address_snapshot",),
}

_DECIMAL_COLUMNS = {
    "unit_price",
    "compare_at_price",
    "discount_value",
    "max_discount_amount",
    "minimum_order_amount",
    "subtotal_amount",
    "shipping_amount",
    "tax_amount",
    "discount_amount",
    "total_amount",
    "item_subtotal",
    "item_total",
    "amount",
}
_INTEGER_COLUMNS = {"quantity", "stock_quantity", "reserved_quantity", "usage_limit", "used_count"}
_TIMESTAMP_COLUMNS = {
    "created_at",
    "updated_at",
    "last_login",
    "starts_at",
    "ends_at",
    "paid_at",
    "shipped_at",
    "delivered_at",
}


@dataclass(frozen=True)
class TypedCdcTableContract:
    table_name: str
    primary_keys: tuple[str, ...]
    payload_schema: StructType

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.payload_schema)


def _source_data_type(column: str) -> DataType:
    if column == "weight_kg":
        return DecimalType(8, 3)
    if column in _DECIMAL_COLUMNS:
        return DecimalType(12, 2)
    if column in _INTEGER_COLUMNS or (column.endswith("_id") and not column.startswith("public_")):
        return IntegerType()
    if column in _TIMESTAMP_COLUMNS:
        return TimestampType()
    if column.startswith("is_"):
        return BooleanType()
    return StringType()


def _payload_schema(table_name: str) -> StructType:
    columns = (*SILVER_TABLES[table_name].columns, *_BRONZE_ONLY_COLUMNS[table_name])
    if len(columns) != len(set(columns)):
        raise RuntimeError(f"Duplicate typed CDC columns for table: {table_name}")
    return StructType([StructField(column, _source_data_type(column)) for column in columns])


if set(BRONZE_TABLES) != set(SILVER_TABLES) or set(BRONZE_TABLES) != set(_BRONZE_ONLY_COLUMNS):
    raise RuntimeError("CDC, Bronze and Silver table contracts must describe the same source tables")


TYPED_CDC_TABLES: dict[str, TypedCdcTableContract] = {
    table_name: TypedCdcTableContract(
        table_name=table_name,
        primary_keys=bronze_contract.primary_keys,
        payload_schema=_payload_schema(table_name),
    )
    for table_name, bronze_contract in BRONZE_TABLES.items()
}


def get_typed_cdc_contract(table_name: str) -> TypedCdcTableContract:
    try:
        return TYPED_CDC_TABLES[table_name]
    except KeyError as exc:
        raise ValueError(f"Unknown typed CDC table: {table_name}") from exc


__all__ = [
    "BRONZE_SCHEMA_VERSION",
    "TYPED_CDC_TABLES",
    "TypedCdcTableContract",
    "get_typed_cdc_contract",
]
