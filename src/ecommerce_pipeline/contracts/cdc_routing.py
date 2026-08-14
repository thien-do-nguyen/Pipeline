from __future__ import annotations

from ecommerce_pipeline.contracts.bronze_tables import BRONZE_TABLES

CDC_DOMAIN_TABLES: dict[str, tuple[str, ...]] = {
    "customer": ("app_users", "user_addresses"),
    "catalog": ("shops", "categories", "products", "product_variants"),
    "promotion": ("vouchers",),
    "sales": ("orders", "order_items", "order_vouchers"),
    "payment": ("payments",),
    "shipping": ("shipments",),
}

assert set().union(*map(set, CDC_DOMAIN_TABLES.values())) == set(BRONZE_TABLES)


def cdc_topic_pattern() -> str:
    return r"ecommerce\.domain\..*"


def cdc_topic_for_table(table_name: str) -> str:
    for domain, tables in CDC_DOMAIN_TABLES.items():
        if table_name in tables:
            return f"ecommerce.domain.{domain}"
    raise ValueError(f"Table is not mapped to a CDC domain: {table_name}")


__all__ = ["CDC_DOMAIN_TABLES", "cdc_topic_for_table", "cdc_topic_pattern"]
