from __future__ import annotations

from typing import Any

from pyspark.sql.streaming.query import StreamingQuery


def summarize_streaming_progress(query: StreamingQuery) -> dict[str, int | float]:
    """Reduce Spark progress events to stable metrics suitable for job logs."""

    recent = getattr(query, "recentProgress", None)
    progress_events = list(recent) if recent else []
    if not progress_events:
        last = getattr(query, "lastProgress", None)
        if last is not None:
            progress_events = [last]

    input_rows = 0
    max_trigger_ms = 0
    max_offsets_behind_latest = 0.0
    latest_offsets_behind_latest = 0.0
    for progress in progress_events:
        input_rows += int(progress.get("numInputRows", 0))
        duration = progress.get("durationMs")
        if isinstance(duration, dict):
            max_trigger_ms = max(max_trigger_ms, int(duration.get("triggerExecution", 0) or 0))
        sources = progress.get("sources")
        if isinstance(sources, list):
            latest_offsets_behind_latest = 0.0
            for source in sources:
                if isinstance(source, dict):
                    source_lag = _source_lag(source)
                    latest_offsets_behind_latest = max(latest_offsets_behind_latest, source_lag)
                    max_offsets_behind_latest = max(max_offsets_behind_latest, source_lag)
    return {
        "micro_batches": len(progress_events),
        "input_rows": input_rows,
        "max_trigger_ms": max_trigger_ms,
        "max_offsets_behind_latest": max_offsets_behind_latest,
        "latest_offsets_behind_latest": latest_offsets_behind_latest,
    }


def _source_lag(source: dict[str, Any]) -> float:
    metrics = source.get("metrics")
    if not isinstance(metrics, dict):
        return 0.0
    raw = metrics.get("maxOffsetsBehindLatest", 0)
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.0


__all__ = ["summarize_streaming_progress"]
