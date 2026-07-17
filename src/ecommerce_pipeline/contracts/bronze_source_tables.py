from dataclasses import dataclass

from ecommerce_pipeline.contracts.silver_source_tables import SOURCE_TABLES


@dataclass(frozen=True)
class BronzeTableContract:
    table_name: str
    primary_keys: tuple[str, ...]
    incremental_column: str
    source_columns: tuple[str, ...]


def _contract(table_name: str, primary_key: str) -> BronzeTableContract:
    return BronzeTableContract(
        table_name=table_name,
        primary_keys=(primary_key,),
        incremental_column="updated_at",
        source_columns=SOURCE_TABLES[table_name].columns,
    )


BRONZE_SOURCE_TABLES: dict[str, BronzeTableContract] = {
    "app_users": _contract("app_users", "user_id"),
    "user_addresses": _contract("user_addresses", "address_id"),
    "shops": _contract("shops", "shop_id"),
    "categories": _contract("categories", "category_id"),
    "products": _contract("products", "product_id"),
    "product_variants": _contract("product_variants", "product_variant_id"),
    "vouchers": _contract("vouchers", "voucher_id"),
    "orders": _contract("orders", "order_id"),
    "order_items": _contract("order_items", "order_item_id"),
    "order_vouchers": _contract("order_vouchers", "order_voucher_id"),
    "payments": _contract("payments", "payment_id"),
    "shipments": _contract("shipments", "shipment_id"),
}


def get_bronze_contract(table_name: str) -> BronzeTableContract:
    if table_name not in BRONZE_SOURCE_TABLES:
        raise ValueError(f"Bronze source table is not declared: {table_name}")
    return BRONZE_SOURCE_TABLES[table_name]


def get_all_bronze_source_tables() -> list[str]:
    return list(BRONZE_SOURCE_TABLES)
