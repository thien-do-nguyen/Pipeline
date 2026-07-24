from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.config.models import AppConfig


class PostgresReader:
    """Read bounded change-event queries from PostgreSQL."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config

    def read_query(self, query: str) -> DataFrame:
        """Read a bounded PostgreSQL query through Spark's batch JDBC source."""
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config.postgres.jdbc_url)
            .option("query", query)
            .option("user", self.config.postgres.user)
            .option("password", self.config.postgres.password_value)
            .option("driver", self.config.postgres.jdbc_driver)
            .option("fetchsize", str(self.config.postgres.fetch_size))
            .load()
        )
