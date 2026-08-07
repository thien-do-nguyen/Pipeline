from pyspark.sql import DataFrame, SparkSession

from ecommerce_pipeline.config.models import KafkaSourceConfig


def read_kafka_stream(spark: SparkSession, config: KafkaSourceConfig) -> DataFrame:
    """Build a Kafka-protocol stream usable with Kafka or Azure Event Hubs."""

    reader = spark.readStream.format("kafka")
    for key, value in config.spark_options().items():
        reader = reader.option(key, value)
    return reader.load()
