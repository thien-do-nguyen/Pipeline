from __future__ import annotations

from unittest.mock import Mock

from ecommerce_pipeline.jobs.run_silver_streaming import _await_continuous_query, _format_progress, _should_log_progress


def test_format_progress_includes_batch_and_input_rows() -> None:
    message = _format_progress(
        {
            "batchId": 7,
            "numInputRows": 42,
            "processedRowsPerSecond": 21.5,
            "durationMs": {"triggerExecution": 1234},
        }
    )

    assert message == (
        "[unified-silver-progress] batch=7 input_rows=42 processed_rows_per_second=21.50 trigger_ms=1234"
    )


def test_continuous_query_logs_waiting_when_no_progress(capsys) -> None:
    query = _FakeQuery([None, None], terminate_on_call=2)
    materializer = Mock()

    _await_continuous_query(query, materializer)

    assert "[unified-silver-progress] status=waiting_for_raw_bronze input_rows=0" in capsys.readouterr().out
    materializer.reconcile_pending_gold_if_ready.assert_called_once_with(batch_id="cdc-idle-reconcile")


def test_zero_input_progress_is_throttled() -> None:
    assert _format_progress({"batchId": 10, "numInputRows": 0}) == (
        "[unified-silver-progress] status=waiting_for_raw_bronze batch=10 input_rows=0"
    )
    assert _should_log_progress({"batchId": 9, "numInputRows": 0}) is False
    assert _should_log_progress({"batchId": 10, "numInputRows": 0}) is True
    assert _should_log_progress({"batchId": 11, "numInputRows": 1}) is True


class _FakeQuery:
    def __init__(self, progress_by_call: list[dict[str, object] | None], *, terminate_on_call: int) -> None:
        self.progress_by_call = progress_by_call
        self.terminate_on_call = terminate_on_call
        self.calls = 0
        self.lastProgress: dict[str, object] | None = None

    @property
    def isActive(self) -> bool:
        return self.calls < self.terminate_on_call

    def awaitTermination(self, timeout: int) -> bool:
        assert timeout == 10
        self.calls += 1
        self.lastProgress = self.progress_by_call[self.calls - 1]
        return self.calls >= self.terminate_on_call
