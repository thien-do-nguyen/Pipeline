from __future__ import annotations

from dataclasses import dataclass

BRONZE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class BronzeTableContract:
    table_name: str
    primary_keys: tuple[str, ...]
    excluded_columns: tuple[str, ...] = ()


BRONZE_TABLES: dict[str, BronzeTableContract] = {
    "app_users": BronzeTableContract("app_users", ("user_id",), ("password_hash",)),
    "user_addresses": BronzeTableContract("user_addresses", ("address_id",)),
    "shops": BronzeTableContract("shops", ("shop_id",)),
    "categories": BronzeTableContract("categories", ("category_id",)),
    "products": BronzeTableContract("products", ("product_id",)),
    "product_variants": BronzeTableContract("product_variants", ("product_variant_id",)),
    "vouchers": BronzeTableContract("vouchers", ("voucher_id",)),
    "orders": BronzeTableContract("orders", ("order_id",)),
    "order_items": BronzeTableContract("order_items", ("order_item_id",)),
    "order_vouchers": BronzeTableContract("order_vouchers", ("order_voucher_id",)),
    "payments": BronzeTableContract("payments", ("payment_id",)),
    "shipments": BronzeTableContract("shipments", ("shipment_id",)),
}


def get_bronze_contract(table_name: str) -> BronzeTableContract:
    try:
        return BRONZE_TABLES[table_name]
    except KeyError as exc:
        raise ValueError(f"Unknown Bronze table: {table_name}") from exc
