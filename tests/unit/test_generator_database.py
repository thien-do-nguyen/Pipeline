from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

from ecommerce_pipeline.generator.database import many_returning
from ecommerce_pipeline.generator.orders import update_product_variant


def test_many_returning_collects_one_row_per_parameter_set() -> None:
    cursor = Mock()
    first = Mock()
    second = Mock()
    first.fetchone.return_value = {"id": 10}
    second.fetchone.return_value = {"id": 11}
    cursor.results.return_value = iter((first, second))
    params = [(1,), (2,)]

    rows = many_returning(cursor, "INSERT ... RETURNING id", params)

    cursor.executemany.assert_called_once_with("INSERT ... RETURNING id", params, returning=True)
    assert rows == [{"id": 10}, {"id": 11}]


def test_variant_price_update_preserves_compare_price_constraint() -> None:
    cursor = Mock()
    variant = SimpleNamespace(unit_price=Decimal("100.00"), product_variant_id=6)
    rng = Mock()
    rng.choice.side_effect = [variant, 1.03]
    rng.randint.return_value = 5
    changed_at = datetime(2026, 8, 5, 9)

    assert update_product_variant(cursor, rng, SimpleNamespace(variants=[variant]), changed_at) == 6

    query, parameters = cursor.execute.call_args.args
    assert "GREATEST(compare_at_price, %s)" in query
    assert parameters == (Decimal("103.00"), Decimal("103.00"), 5, changed_at, 6)
