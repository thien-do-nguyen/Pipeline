from __future__ import annotations

from pyspark.sql import DataFrame

from ecommerce_pipeline.contracts.silver_source_tables import SilverTableContract
from ecommerce_pipeline.transformations.silver.common import (
    clean_text,
    current_state,
    normalize_lower,
    normalize_upper,
)

SALES_TABLES = {
    "shops",
    "categories",
    "products",
    "product_variants",
    "vouchers",
    "orders",
    "order_items",
    "order_vouchers",
    "payments",
    "shipments",
}

TEXT_COLUMNS = {
    "shop_name",
    "shop_slug",
    "category_name",
    "slug",
    "product_sku",
    "product_slug",
    "product_name",
    "brand",
    "variant_sku",
    "variant_name",
    "voucher_code",
    "voucher_name",
    "order_number",
    "product_name_snapshot",
    "product_sku_snapshot",
    "variant_name_snapshot",
    "variant_sku_snapshot",
    "payment_method",
    "carrier",
    "tracking_number",
}
LOWER_COLUMNS = {"status", "order_status", "payment_status", "shipment_status", "discount_type", "payment_method"}
UPPER_COLUMNS = {
    "currency",
    "product_sku",
    "variant_sku",
    "voucher_code",
    "product_sku_snapshot",
    "variant_sku_snapshot",
}


def supports_sales_table(table_name: str) -> bool:
    return table_name in SALES_TABLES


def transform_sales_table(df: DataFrame, contract: SilverTableContract) -> DataFrame:
    if not supports_sales_table(contract.table_name):
        raise ValueError(f"Unsupported sales silver table: {contract.table_name}")
    cleaned = current_state(df, contract)
    cleaned = clean_text(cleaned, sorted(TEXT_COLUMNS & set(cleaned.columns)))
    cleaned = normalize_lower(cleaned, sorted(LOWER_COLUMNS & set(cleaned.columns)))
    cleaned = normalize_upper(cleaned, sorted(UPPER_COLUMNS & set(cleaned.columns)))
    return cleaned
