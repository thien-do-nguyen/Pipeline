from __future__ import annotations

from py4j.protocol import Py4JError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming.query import StreamingQuery

_JVM_DISCONNECT_ERRORS = (Py4JError, ConnectionError, EOFError)


def log_runtime_failure(label: str, error: Exception) -> None:
    """Emit one compact root-failure marker before cleanup starts."""

    try:
        message = " ".join(str(error).split())[:500]
    except Exception:
        message = "unavailable"
    print(f"[{label}] status=FAILED error_type={type(error).__name__} error={message}", flush=True)


def stop_streaming_query_safely(query: StreamingQuery, *, label: str) -> None:
    """Stop an active query without replacing the original JVM failure."""

    try:
        if query.isActive:
            query.stop()
    except _JVM_DISCONNECT_ERRORS:
        print(f"[{label}] cleanup=query_stop_skipped reason=jvm_unavailable", flush=True)


def stop_spark_safely(spark: SparkSession, *, label: str) -> None:
    """Stop Spark when reachable; a dead gateway is already the root failure."""

    try:
        spark.stop()
    except _JVM_DISCONNECT_ERRORS:
        print(f"[{label}] cleanup=spark_stop_skipped reason=jvm_unavailable", flush=True)


def unpersist_safely(dataframe: DataFrame, *, label: str) -> None:
    """Release a cache without masking an earlier disconnected-JVM failure."""

    try:
        dataframe.unpersist()
    except _JVM_DISCONNECT_ERRORS:
        print(f"[{label}] cleanup=unpersist_skipped reason=jvm_unavailable", flush=True)


__all__ = ["log_runtime_failure", "stop_spark_safely", "stop_streaming_query_safely", "unpersist_safely"]
