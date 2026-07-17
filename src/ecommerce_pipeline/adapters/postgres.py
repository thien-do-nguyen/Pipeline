from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.config.models import AppConfig


class PostgresReader:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config

    def read_table(self, schema: str, table_name: str) -> DataFrame:
        dbtable = f"{schema}.{table_name}"
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config.postgres.jdbc_url)
            .option("dbtable", dbtable)
            .option("user", self.config.postgres.user)
            .option("password", self.config.postgres.password_value)
            .option("driver", self.config.postgres.jdbc_driver)
            .option("fetchsize", str(self.config.postgres.fetch_size))
            .load()
        )

    def read_query(self, query: str) -> DataFrame:
        """
        Đọc dữ liệu từ PostgreSQL dựa trên một câu truy vấn SQL tùy chỉnh.
        Dùng cho cơ chế Incremental Load (kết hợp với Watermark).
        """
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
