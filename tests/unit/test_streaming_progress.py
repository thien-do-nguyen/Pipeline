from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from pyspark.sql.streaming.query import StreamingQuery

from ecommerce_pipeline.runtime.streaming_progress import summarize_streaming_progress


def test_streaming_progress_summarizes_rows_duration_and_latest_lag() -> None:
    query = SimpleNamespace(
        recentProgress=[
            {
                "numInputRows": 10,
                "durationMs": {"triggerExecution": 1200},
                "sources": [{"metrics": {"maxOffsetsBehindLatest": "7"}}],
            },
            {
                "numInputRows": 7,
                "durationMs": {"triggerExecution": 800},
                "sources": [{"metrics": {"maxOffsetsBehindLatest": "0"}}],
            },
        ]
    )

    metrics = summarize_streaming_progress(cast(StreamingQuery, query))

    assert metrics == {
        "micro_batches": 2,
        "input_rows": 17,
        "max_trigger_ms": 1200,
        "max_offsets_behind_latest": 7.0,
        "latest_offsets_behind_latest": 0.0,
    }


def test_streaming_progress_uses_last_progress_when_recent_is_empty() -> None:
    query = SimpleNamespace(recentProgress=[], lastProgress={"numInputRows": 0})

    metrics = summarize_streaming_progress(cast(StreamingQuery, query))

    assert metrics["micro_batches"] == 1
    assert metrics["input_rows"] == 0
