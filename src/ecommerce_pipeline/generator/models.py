from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

AddressSnapshot = dict[str, object]


def money(value: float | int | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")


def require_non_empty(name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


@dataclass(frozen=True)
class Variant:
    product_id: int
    product_variant_id: int
    shop_id: int
    product_name: str
    product_sku: str
    variant_name: str
    variant_sku: str
    unit_price: Decimal

    def __post_init__(self) -> None:
        require_positive("product_id", self.product_id)
        require_positive("product_variant_id", self.product_variant_id)
        require_positive("shop_id", self.shop_id)
        require_non_empty("product_name", self.product_name)
        require_non_empty("product_sku", self.product_sku)
        require_non_empty("variant_name", self.variant_name)
        require_non_empty("variant_sku", self.variant_sku)
        if self.unit_price < 0:
            raise ValueError("unit_price must be greater than or equal to 0")


@dataclass(frozen=True)
class RuntimeState:
    user_ids: list[int]
    address_by_user: dict[int, int]
    address_snapshot_by_user: dict[int, AddressSnapshot]
    variants: list[Variant]
    user_created_at: dict[int, datetime]
    voucher_ids: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.user_ids:
            raise ValueError("user_ids must not be empty")
        if not self.address_by_user:
            raise ValueError("address_by_user must not be empty")
        if not self.variants:
            raise ValueError("variants must not be empty")
        for user_id in self.user_ids:
            require_positive("user_id", user_id)
            if user_id not in self.address_by_user:
                raise ValueError(f"missing address for user_id={user_id}")
            if user_id not in self.address_snapshot_by_user:
                raise ValueError(f"missing address snapshot for user_id={user_id}")
            if user_id not in self.user_created_at:
                raise ValueError(f"missing created_at for user_id={user_id}")
        for address_id in self.address_by_user.values():
            require_positive("address_id", address_id)
        for voucher_id in self.voucher_ids:
            require_positive("voucher_id", voucher_id)


@dataclass(frozen=True)
class SeedPlan:
    customers: int = 100
    orders: int = 500

    def __post_init__(self) -> None:
        require_positive("customers", self.customers)
        require_positive("orders", self.orders)


@dataclass(frozen=True)
class StreamPlan:
    orders_per_batch: int = 5
    interval_seconds: float = 10.0
    max_batches: int | None = None

    def __post_init__(self) -> None:
        require_positive("orders_per_batch", self.orders_per_batch)
        if self.interval_seconds < 0:
            raise ValueError("interval_seconds must be greater than or equal to 0")
        if self.max_batches is not None:
            require_positive("max_batches", self.max_batches)
