from unittest.mock import Mock

from ecommerce_pipeline.control.gold_reconcile_queue import GoldReconcileQueue


def test_invalid_order_scope_falls_back_to_full_reconciliation() -> None:
    queue = object.__new__(GoldReconcileQueue)
    queue.spark = Mock()
    dataframe = queue.spark.createDataFrame.return_value
    queue.lakehouse = Mock()
    queue.lakehouse.table_exists.return_value = False

    queue.enqueue(
        request_id="request-1",
        affected_order_ids={0, 42},
        requires_fact_readiness=True,
    )

    rows = queue.spark.createDataFrame.call_args.args[0]
    assert len(rows) == 1
    assert rows[0][0:4] == ("request-1", "all-orders", None, True)
    queue.lakehouse.write_table.assert_called_once_with(
        dataframe,
        "silver",
        "gold_reconcile_queue",
    )
