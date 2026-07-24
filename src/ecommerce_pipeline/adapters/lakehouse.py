from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.config.models import AppConfig

DELTA_COMMIT_METADATA_KEY = "spark.databricks.delta.commitInfo.userMetadata"


@contextmanager
def delta_commit_metadata(spark: SparkSession, metadata: dict[str, object]) -> Iterator[None]:
    """Attach pipeline progress to the same Delta transaction as the data write."""

    previous = spark.conf.get(DELTA_COMMIT_METADATA_KEY, None)
    spark.conf.set(DELTA_COMMIT_METADATA_KEY, json.dumps(metadata, separators=(",", ":"), sort_keys=True))
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(DELTA_COMMIT_METADATA_KEY)
        else:
            spark.conf.set(DELTA_COMMIT_METADATA_KEY, previous)


def latest_delta_version(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).history(1).select("version").first()
    if row is None:
        raise RuntimeError(f"Delta history is empty: {path}")
    return int(row["version"])


def latest_commit_metadata(
    spark: SparkSession,
    path: str,
    *,
    pipeline: str,
) -> dict[str, object] | None:
    """Read the newest progress marker written by a specific pipeline."""

    from delta.tables import DeltaTable

    row = (
        DeltaTable.forPath(spark, path)
        .history()
        .where(F.get_json_object(F.col("userMetadata"), "$.pipeline") == F.lit(pipeline))
        .orderBy(F.col("version").desc())
        .select("userMetadata")
        .first()
    )
    if row is None or row["userMetadata"] is None:
        return None
    payload = json.loads(row["userMetadata"])
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid Delta commit metadata at {path}")
    return payload


def ensure_change_data_feed(spark: SparkSession, path: str) -> None:
    """Enable CDF once; future Bronze commits can then be read by Delta version."""

    from delta.tables import DeltaTable

    detail = DeltaTable.forPath(spark, path).detail().select("properties").first()
    properties = {} if detail is None else detail["properties"] or {}
    if str(properties.get("delta.enableChangeDataFeed", "false")).lower() != "true":
        set_delta_table_property(spark, path, "delta.enableChangeDataFeed", "true")


def set_delta_table_property(spark: SparkSession, path: str, key: str, value: str) -> None:
    spark.sql(
        f"ALTER TABLE {_delta_sql_identifier(path)} SET TBLPROPERTIES ({_sql_literal(key)} = {_sql_literal(value)})"
    )


class LakehouseAdapter:
    """Delta Lake persistence adapter."""

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
        *,
        enable_change_data_feed: bool = False,
    ) -> None:
        path = self.config.lakehouse.table_path(layer, table_name)
        writer = df.write.format(self.config.lakehouse.format).mode(mode).option("overwriteSchema", "true")
        if enable_change_data_feed:
            writer = writer.option("delta.enableChangeDataFeed", "true")
        writer.save(path)

    def delete_matching(
        self,
        keys: DataFrame,
        layer: str,
        table_name: str,
        merge_keys: Sequence[str],
    ) -> None:
        """Delete target rows whose keys occur in a bounded incremental key set."""

        if not merge_keys:
            raise ValueError("merge_keys must not be empty")
        if not self.table_exists(layer, table_name):
            return
        missing = sorted(set(merge_keys) - set(keys.columns))
        if missing:
            raise ValueError(f"Missing delete keys for {layer}.{table_name}: {missing}")

        from delta.tables import DeltaTable

        path = self.config.lakehouse.table_path(layer, table_name)
        distinct_keys = keys.select(*merge_keys).dropDuplicates()
        join_condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in merge_keys)
        (
            DeltaTable.forPath(self.spark, path)
            .alias("target")
            .merge(distinct_keys.alias("source"), join_condition)
            .whenMatchedDelete()
            .execute()
        )

    def upsert_table(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        merge_keys: Sequence[str],
        delete_mode: Literal["soft", "hard"] = "soft",
        delete_not_matched_by_source: bool = False,
    ) -> None:
        if not merge_keys:
            raise ValueError("merge_keys must not be empty")
        if delete_mode not in {"soft", "hard"}:
            raise ValueError("delete_mode must be either 'soft' or 'hard'")

        path = self.config.lakehouse.table_path(layer, table_name)

        from delta.tables import DeltaTable

        source_columns = set(df.columns)
        hard_delete_column = delete_mode == "hard" and "_operation" in source_columns

        if not DeltaTable.isDeltaTable(self.spark, path):
            if hard_delete_column:
                df = df.filter(F.coalesce(F.col("_operation") != F.lit("DELETE"), F.lit(True)))
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
        not_delete_condition = "(source.`_operation` IS NULL OR source.`_operation` <> 'DELETE')"
        if hard_delete_column:
            merge = merge.whenMatchedDelete(condition="source.`_operation` = 'DELETE'")
        if compared:
            equality = " AND ".join(f"target.`{name}` <=> source.`{name}`" for name in compared)
            update_condition = f"NOT ({equality})"
            if hard_delete_column:
                update_condition = f"{not_delete_condition} AND {update_condition}"
            merge = merge.whenMatchedUpdate(condition=update_condition, set=update_values)
        insert_condition = not_delete_condition if hard_delete_column else None
        if insert_condition is None:
            merge = merge.whenNotMatchedInsertAll()
        else:
            merge = merge.whenNotMatchedInsertAll(condition=insert_condition)
        if delete_not_matched_by_source:
            merge = merge.whenNotMatchedBySourceDelete()
        merge.execute()

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


def _delta_sql_identifier(path: str) -> str:
    normalized = path if urlparse(path).scheme else Path(path).resolve().as_uri()
    return f"delta.`{normalized.replace('`', '``')}`"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
