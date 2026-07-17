from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverTableContract:
    table_name: str
    primary_keys: tuple[str, ...]
    columns: tuple[str, ...]
    incremental_column: str = "updated_at"


SOURCE_TABLES: dict[str, SilverTableContract] = {
    "app_users": SilverTableContract(
        "app_users",
        ("user_id",),
        (
            "user_id",
            "public_user_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "phone_number",
            "status",
            "created_at",
            "updated_at",
            "last_login",
        ),
    ),
    "user_addresses": SilverTableContract(
        "user_addresses",
        ("address_id",),
        (
            "address_id",
            "user_id",
            "address_type",
            "recipient_name",
            "phone_number",
            "street",
            "ward",
            "district",
            "city",
            "state",
            "postal_code",
            "country",
            "created_at",
            "updated_at",
        ),
    ),
    "shops": SilverTableContract(
        "shops",
        ("shop_id",),
        ("shop_id", "public_shop_id", "shop_name", "shop_slug", "status", "created_at", "updated_at"),
    ),
    "categories": SilverTableContract(
        "categories",
        ("category_id",),
        ("category_id", "parent_category_id", "category_name", "slug", "is_active", "created_at", "updated_at"),
    ),
    "products": SilverTableContract(
        "products",
        ("product_id",),
        (
            "product_id",
            "public_product_id",
            "shop_id",
            "category_id",
            "product_sku",
            "product_slug",
            "product_name",
            "brand",
            "attributes_json",
            "images_json",
            "status",
            "is_featured",
            "created_at",
            "updated_at",
        ),
    ),
    "product_variants": SilverTableContract(
        "product_variants",
        ("product_variant_id",),
        (
            "product_variant_id",
            "public_variant_id",
            "product_id",
            "variant_sku",
            "variant_name",
            "options_json",
            "unit_price",
            "compare_at_price",
            "currency",
            "stock_quantity",
            "reserved_quantity",
            "weight_kg",
            "images_json",
            "status",
            "is_default",
            "created_at",
            "updated_at",
        ),
    ),
    "vouchers": SilverTableContract(
        "vouchers",
        ("voucher_id",),
        (
            "voucher_id",
            "voucher_code",
            "voucher_name",
            "shop_id",
            "discount_type",
            "discount_value",
            "max_discount_amount",
            "minimum_order_amount",
            "scope_json",
            "starts_at",
            "ends_at",
            "is_active",
            "created_at",
            "updated_at",
        ),
    ),
    "orders": SilverTableContract(
        "orders",
        ("order_id",),
        (
            "order_id",
            "public_order_id",
            "customer_id",
            "order_number",
            "order_status",
            "payment_status",
            "shipping_address_id",
            "billing_address_id",
            "shipping_address_snapshot",
            "billing_address_snapshot",
            "subtotal_amount",
            "shipping_amount",
            "tax_amount",
            "discount_amount",
            "total_amount",
            "currency",
            "created_at",
            "updated_at",
        ),
    ),
    "order_items": SilverTableContract(
        "order_items",
        ("order_item_id",),
        (
            "order_item_id",
            "order_id",
            "shop_id",
            "product_id",
            "product_variant_id",
            "product_name_snapshot",
            "product_sku_snapshot",
            "variant_name_snapshot",
            "variant_sku_snapshot",
            "variant_options_snapshot",
            "quantity",
            "unit_price",
            "currency",
            "tax_amount",
            "discount_amount",
            "item_subtotal",
            "item_total",
            "created_at",
            "updated_at",
        ),
    ),
    "order_vouchers": SilverTableContract(
        "order_vouchers",
        ("order_voucher_id",),
        ("order_voucher_id", "order_id", "voucher_id", "discount_amount", "created_at", "updated_at"),
    ),
    "payments": SilverTableContract(
        "payments",
        ("payment_id",),
        (
            "payment_id",
            "public_payment_id",
            "order_id",
            "payment_method",
            "payment_status",
            "amount",
            "currency",
            "paid_at",
            "created_at",
            "updated_at",
        ),
    ),
    "shipments": SilverTableContract(
        "shipments",
        ("shipment_id",),
        (
            "shipment_id",
            "public_shipment_id",
            "order_id",
            "shipment_status",
            "carrier",
            "tracking_number",
            "shipped_at",
            "delivered_at",
            "created_at",
            "updated_at",
        ),
    ),
}


def get_contract(table_name: str) -> SilverTableContract:
    try:
        return SOURCE_TABLES[table_name]
    except KeyError as exc:
        raise ValueError(f"Unknown silver table: {table_name}") from exc


def get_table_contracts(table_names: list[str] | None = None) -> list[SilverTableContract]:
    names = list(SOURCE_TABLES) if table_names is None else table_names
    return [get_contract(name) for name in names]
