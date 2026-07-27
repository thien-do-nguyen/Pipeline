from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

from pyspark.sql import DataFrame, DataFrameReader, Row, SparkSession

from ecommerce_pipeline.config.models import AppConfig

T = TypeVar("T")
logger = logging.getLogger(__name__)


class PostgresReader:
    """Read bounded change-event queries from PostgreSQL."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config

    def read_query(self, query: str) -> DataFrame:
        """Read a bounded PostgreSQL query through Spark's batch JDBC source."""
        return self._reader().option("query", query).load()

    def read_partitioned_query(
        self,
        query: str,
        *,
        partition_column: str,
        lower_bound: int,
        upper_bound: int,
        num_partitions: int,
    ) -> DataFrame:
        """Read a bounded subquery in parallel without exceeding the configured connection cap."""

        if lower_bound >= upper_bound:
            raise ValueError("JDBC lower_bound must be smaller than upper_bound")
        if not 1 <= num_partitions <= self.config.postgres.max_jdbc_partitions:
            raise ValueError("JDBC num_partitions is outside the configured source throttle")
        return (
            self._reader()
            .option("dbtable", f"({query}) AS source_events")
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(num_partitions))
            .load()
        )

    def read_first(self, query: str) -> Row | None:
        """Execute a small control query with bounded exponential retry."""

        return self.with_retry(lambda: self.read_query(query).first(), operation="JDBC control query")

    def with_retry(self, action: Callable[[], T], *, operation: str) -> T:
        attempts = self.config.postgres.retry_attempts
        initial_backoff = self.config.postgres.retry_initial_backoff_seconds
        for attempt in range(1, attempts + 1):
            try:
                return action()
            except Exception:
                if attempt == attempts:
                    raise
                delay = initial_backoff * (2 ** (attempt - 1))
                logger.warning(
                    "%s failed; retrying attempt %s/%s in %.1fs",
                    operation,
                    attempt + 1,
                    attempts,
                    delay,
                )
                time.sleep(delay)
        raise AssertionError("unreachable")

    def _reader(self) -> DataFrameReader:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config.postgres.jdbc_url)
            .option("user", self.config.postgres.user)
            .option("password", self.config.postgres.password_value)
            .option("driver", self.config.postgres.jdbc_driver)
            .option("fetchsize", str(self.config.postgres.fetch_size))
            .option("queryTimeout", str(self.config.postgres.query_timeout_seconds))
            .option("ApplicationName", self.config.application.name)
        )
