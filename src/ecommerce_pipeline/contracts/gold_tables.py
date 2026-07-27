from __future__ import annotations

from dataclasses import dataclass

GOLD_TABLES = (
    "dim_customer",
    "dim_date",
    "dim_time",
    "dim_location",
    "dim_shop",
    "dim_category",
    "dim_product",
    "dim_promotion",
    "dim_payment",
    "dim_shipping",
    "fact_sales",
)


@dataclass(frozen=True)
class Scd2DimensionContract:
    source_key: str
    surrogate_key: str
    initial_effective_from: str
    type1_columns: tuple[str, ...] = ()
    attribute_hash: str = "attribute_hash"

    @property
    def required_columns(self) -> set[str]:
        return {
            self.source_key,
            self.surrogate_key,
            self.attribute_hash,
            self.initial_effective_from,
            "effective_from",
            "effective_to",
            "is_current",
        }


SCD2_DIMENSIONS = {
    "dim_customer": Scd2DimensionContract(
        source_key="source_customer_id",
        surrogate_key="customer_key",
        initial_effective_from="registered_at",
        type1_columns=("last_login_at",),
    ),
    "dim_product": Scd2DimensionContract(
        source_key="source_product_variant_id",
        surrogate_key="product_key",
        initial_effective_from="variant_created_at",
        type1_columns=(
            "current_unit_price",
            "compare_at_price",
            "stock_quantity",
            "reserved_quantity",
        ),
    ),
    "dim_shop": Scd2DimensionContract(
        source_key="source_shop_id",
        surrogate_key="shop_key",
        initial_effective_from="shop_created_at",
    ),
    "dim_category": Scd2DimensionContract(
        source_key="source_category_id",
        surrogate_key="category_key",
        initial_effective_from="category_created_at",
    ),
}
