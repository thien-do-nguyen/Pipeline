from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.config.models import AppConfig


class LakehouseAdapter:
    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self.spark = spark
        self.config = config

    def read_table(self, layer: str, table_name: str) -> DataFrame:
        path = self.config.lakehouse.table_path(layer, table_name)
        return self.spark.read.format(self.config.lakehouse.format).load(path)

    def table_exists(self, layer: str, table_name: str) -> bool:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(self.spark, self.config.lakehouse.table_path(layer, table_name))

    def write_table(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        path = self.config.lakehouse.table_path(layer, table_name)
        df.write.format(self.config.lakehouse.format).mode(mode).option("overwriteSchema", "true").save(path)

    def upsert_table(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        merge_keys: Sequence[str],
    ) -> None:
        if not merge_keys:
            raise ValueError("merge_keys must not be empty")

        path = self.config.lakehouse.table_path(layer, table_name)

        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(self.spark, path):
            self.write_table(df, layer, table_name)
            return

        join_condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in merge_keys)
        source_columns = set(df.columns)
        target_columns = set(self.read_table(layer, table_name).columns)
        if source_columns != target_columns:
            missing_from_source = sorted(target_columns - source_columns)
            missing_from_target = sorted(source_columns - target_columns)
            raise ValueError(
                f"Schema mismatch for {layer}.{table_name}; "
                f"missing_from_source={missing_from_source}, missing_from_target={missing_from_target}. "
                "Rebuild the affected layer explicitly."
            )

        ignored_for_comparison = {*merge_keys, "created_at", "_silver_updated_at"}
        if layer == "gold":
            ignored_for_comparison.add("updated_at")
        compared = sorted(source_columns - ignored_for_comparison)
        update_columns = sorted(source_columns - {"created_at"})
        update_values: dict[str, str | Column] = {name: f"source.`{name}`" for name in update_columns}
        merge = DeltaTable.forPath(self.spark, path).alias("target").merge(df.alias("source"), join_condition)
        if compared:
            equality = " AND ".join(f"target.`{name}` <=> source.`{name}`" for name in compared)
            merge = merge.whenMatchedUpdate(condition=f"NOT ({equality})", set=update_values)
        merge.whenNotMatchedInsertAll().execute()

    def merge_scd2(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        source_key: str,
        surrogate_key: str,
        attribute_hash: str,
        type1_columns: Sequence[str] = (),
    ) -> None:
        from delta.tables import DeltaTable

        path = self.config.lakehouse.table_path(layer, table_name)
        if not DeltaTable.isDeltaTable(self.spark, path):
            initial = df.withColumn(
                "effective_from",
                F.when(
                    F.col(source_key).isNotNull(), F.coalesce(F.col("registered_at"), F.col("effective_from"))
                ).otherwise(F.col("effective_from")),
            )
            self.write_table(initial, layer, table_name)
            return

        target = self.read_table(layer, table_name).filter(F.col("is_current"))
        changed = (
            df.filter(F.col(source_key).isNotNull())
            .alias("source")
            .join(target.alias("target"), source_key, "left")
            .filter(
                F.col(f"target.{source_key}").isNull()
                | ~F.col(f"source.{attribute_hash}").eqNullSafe(F.col(f"target.{attribute_hash}"))
            )
            .select(
                "source.*",
                F.col(f"target.{source_key}").isNull().alias("_is_new_member"),
            )
            .withColumn(
                "effective_from",
                F.when(F.col("_is_new_member"), F.coalesce(F.col("registered_at"), F.col("effective_from"))).otherwise(
                    F.col("effective_from")
                ),
            )
            .drop("_is_new_member")
            # Delta invalidates caches that depend on a table after a write.
            # localCheckpoint truncates that lineage so the close + insert
            # operations consume the exact same SCD2 change set.
            .localCheckpoint(eager=True)
        )
        changed_rows = changed.count()
        delta = DeltaTable.forPath(self.spark, path)
        if changed_rows > 0:
            delta.alias("target").merge(
                changed.alias("source"),
                f"target.{source_key} = source.{source_key} AND target.is_current = true",
            ).whenMatchedUpdate(
                set={
                    "effective_to": "source.effective_from",
                    "is_current": "false",
                    "updated_at": "current_timestamp()",
                }
            ).execute()
            delta.alias("target").merge(
                changed.alias("source"), f"target.{surrogate_key} = source.{surrogate_key}"
            ).whenNotMatchedInsertAll().execute()
        changed.unpersist()

        if type1_columns:
            changed_type1 = " OR ".join(f"NOT (target.`{column}` <=> source.`{column}`)" for column in type1_columns)
            update_type1: dict[str, str | Column] = {column: f"source.`{column}`" for column in type1_columns}
            update_type1["updated_at"] = "current_timestamp()"
            delta.alias("target").merge(
                df.filter(F.col(source_key).isNotNull()).alias("source"),
                f"target.{source_key} = source.{source_key} AND target.is_current = true",
            ).whenMatchedUpdate(
                condition=f"target.`{attribute_hash}` <=> source.`{attribute_hash}` AND ({changed_type1})",
                set=update_type1,
            ).execute()
