from __future__ import annotations

import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from ecommerce_pipeline.config.models import AppConfig

SPARK_WAREHOUSE_CONFIG = "spark.sql.warehouse.dir"


class SparkSessionBuilder:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.builder = SparkSession.builder.appName(config.spark.app_name)

    def _configure_delta_package(self) -> None:
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:
            raise RuntimeError("Vui lòng cài đặt thư viện delta-spark trước khi chạy ứng dụng.") from exc

        extra_packages = [
            package.strip()
            for package in self.config.spark.config.get("spark.jars.packages", "").split(",")
            if package.strip()
        ]

        self.builder = configure_spark_with_delta_pip(self.builder, extra_packages=extra_packages)

    def build(self) -> SparkSession:
        if self.config.spark.configure_delta_package:
            self._configure_delta_package()

        if self.config.spark.master:
            self.builder = self.builder.master(self.config.spark.master)

        for key, value in self.config.spark.config.items():
            if key == "spark.jars.packages" and self.config.spark.configure_delta_package:
                continue
            self.builder = self.builder.config(key, value)

        if self.config.spark.master and SPARK_WAREHOUSE_CONFIG not in self.config.spark.config:
            warehouse = Path(tempfile.gettempdir(), self.config.application.name, "spark-warehouse")
            self.builder = self.builder.config(SPARK_WAREHOUSE_CONFIG, warehouse.as_uri())
        self.builder = self.builder.config("spark.sql.session.timeZone", self.config.application.timezone)

        spark = self.builder.getOrCreate()
        if self.config.spark.master:
            spark.sparkContext.setLogLevel("WARN")
        return spark


def build_spark(config: AppConfig) -> SparkSession:
    return SparkSessionBuilder(config).build()
