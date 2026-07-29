from unittest.mock import Mock

from ecommerce_pipeline.generator.database import many_returning


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
