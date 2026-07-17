from __future__ import annotations

from pyspark.sql import DataFrame

from ecommerce_pipeline.contracts.silver_source_tables import SilverTableContract
from ecommerce_pipeline.transformations.silver.common import (
    clean_text,
    current_state,
    normalize_lower,
    normalize_phone,
)

CUSTOMER_TABLES = {"app_users", "user_addresses"}


def supports_customer_table(table_name: str) -> bool:
    return table_name in CUSTOMER_TABLES


def transform_customer_table(df: DataFrame, contract: SilverTableContract) -> DataFrame:
    if not supports_customer_table(contract.table_name):
        raise ValueError(f"Unsupported customer silver table: {contract.table_name}")
    cleaned = current_state(df, contract)
    if contract.table_name == "app_users":
        cleaned = clean_text(cleaned, ["username", "email", "first_name", "last_name", "phone_number"])
        cleaned = normalize_lower(cleaned, ["username", "email", "status"])
        cleaned = normalize_phone(cleaned)
        return cleaned

    cleaned = clean_text(
        cleaned,
        [
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
        ],
    )
    cleaned = normalize_lower(cleaned, ["address_type"])
    cleaned = normalize_phone(cleaned)
    return cleaned
