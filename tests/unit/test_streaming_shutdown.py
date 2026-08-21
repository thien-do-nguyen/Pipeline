from unittest.mock import Mock, PropertyMock

from py4j.protocol import Py4JNetworkError

from ecommerce_pipeline.runtime.shutdown import (
    log_runtime_failure,
    stop_spark_safely,
    stop_streaming_query_safely,
    unpersist_safely,
)


def test_runtime_failure_marker_survives_broken_exception_string(capsys) -> None:
    error = Mock(spec=Exception)
    error.__str__ = Mock(side_effect=RuntimeError("broken exception formatter"))

    log_runtime_failure("stream", error)

    output = capsys.readouterr().out
    assert "[stream] status=FAILED" in output
    assert "error=unavailable" in output


def test_query_cleanup_does_not_mask_dead_gateway(capsys) -> None:
    query = Mock()
    type(query).isActive = PropertyMock(side_effect=Py4JNetworkError("gateway stopped"))

    stop_streaming_query_safely(query, label="stream")

    assert "cleanup=query_stop_skipped reason=jvm_unavailable" in capsys.readouterr().out


def test_spark_cleanup_does_not_mask_dead_gateway(capsys) -> None:
    spark = Mock()
    spark.stop.side_effect = ConnectionRefusedError(111, "connection refused")

    stop_spark_safely(spark, label="stream")

    assert "cleanup=spark_stop_skipped reason=jvm_unavailable" in capsys.readouterr().out


def test_unpersist_cleanup_does_not_mask_dead_gateway(capsys) -> None:
    dataframe = Mock()
    dataframe.unpersist.side_effect = Py4JNetworkError("gateway stopped")

    unpersist_safely(dataframe, label="stream")

    assert "cleanup=unpersist_skipped reason=jvm_unavailable" in capsys.readouterr().out
