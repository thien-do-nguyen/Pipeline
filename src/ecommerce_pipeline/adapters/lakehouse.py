from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.config.models import AppConfig

DELTA_COMMIT_METADATA_KEY = "spark.databricks.delta.commitInfo.userMetadata"
PROGRESS_HISTORY_FALLBACK_LIMIT = 100


@dataclass(frozen=True)
class DeltaTableState:
    """Latest physical version and latest bounded pipeline commit."""

    version: int
    progress_version: int | None
    progress: dict[str, object] | None


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


def delta_table_state(
    spark: SparkSession,
    path: str,
    *,
    pipeline: str,
) -> DeltaTableState:
    """Read one commit normally and use a bounded maintenance fallback."""

    from delta.tables import DeltaTable

    delta = DeltaTable.forPath(spark, path)
    latest = delta.history(1).select("version", "userMetadata").first()
    if latest is None:
        raise RuntimeError(f"Delta history is empty: {path}")
    latest_version = int(latest["version"])
    progress = _pipeline_metadata(latest["userMetadata"], pipeline)
    if progress is not None:
        return DeltaTableState(latest_version, latest_version, progress)

    rows = delta.history(PROGRESS_HISTORY_FALLBACK_LIMIT).select("version", "userMetadata").collect()
    for row in rows:
        progress = _pipeline_metadata(row["userMetadata"], pipeline)
        if progress is not None:
            return DeltaTableState(latest_version, int(row["version"]), progress)
    return DeltaTableState(latest_version, None, None)


def delta_table_properties(spark: SparkSession, path: str) -> dict[str, str]:
    """Read the current Delta table properties without scanning table data or history."""

    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, path).detail().select("properties").first()
    if row is None:
        raise RuntimeError(f"Delta table detail is empty: {path}")
    return {str(key): str(value) for key, value in (row["properties"] or {}).items()}


def set_delta_table_property(spark: SparkSession, path: str, key: str, value: str) -> None:
    spark.sql(
        f"ALTER TABLE {_delta_sql_identifier(path)} SET TBLPROPERTIES ({_sql_literal(key)} = {_sql_literal(value)})"
    )


class LakehouseAdapter:
    """Delta Lake persistence adapter."""

    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        *,
        gold_versions: Mapping[str, int] | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.gold_versions = None if gold_versions is None else dict(gold_versions)

    def with_gold_versions(self, versions: Mapping[str, int]) -> LakehouseAdapter:
        """Return a reader pinned to one published Gold snapshot."""

        return LakehouseAdapter(self.spark, self.config, gold_versions=versions)

    def read_table(self, layer: str, table_name: str) -> DataFrame:
        path = self.config.lakehouse.table_path(layer, table_name)
        reader = self.spark.read.format(self.config.lakehouse.format)
        if layer == "gold" and self.gold_versions is not None:
            version = self.gold_versions.get(table_name)
            if version is None:
                raise ValueError(f"Gold table is not part of the published snapshot: {table_name}")
            reader = reader.option("versionAsOf", version)
        return reader.load(path)

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

    def upsert_table(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        merge_keys: Sequence[str],
        delete_mode: Literal["soft", "hard"] = "soft",
        delete_not_matched_by_source: bool = False,
        *,
        delete_keys: DataFrame | None = None,
    ) -> None:
        if not merge_keys:
            raise ValueError("merge_keys must not be empty")
        if delete_mode not in {"soft", "hard"}:
            raise ValueError("delete_mode must be either 'soft' or 'hard'")
        if delete_keys is not None:
            missing_delete_keys = sorted(set(merge_keys) - set(delete_keys.columns))
            if missing_delete_keys:
                raise ValueError(f"Missing delete keys for {layer}.{table_name}: {missing_delete_keys}")

        path = self.config.lakehouse.table_path(layer, table_name)

        from delta.tables import DeltaTable

        source_columns = set(df.columns)
        uses_operation_delete = delete_mode == "hard" and "_operation" in source_columns

        if not DeltaTable.isDeltaTable(self.spark, path):
            if uses_operation_delete:
                df = df.filter(F.coalesce(F.col("_operation") != F.lit("DELETE"), F.lit(True)))
            self.write_table(df, layer, table_name)
            return

        join_condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in merge_keys)
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
        changes = df
        delete_condition: str | None = None
        if delete_keys is not None:
            delete_rows = delete_keys.select(*merge_keys).dropDuplicates()
            for field in df.schema:
                if field.name not in merge_keys:
                    delete_rows = delete_rows.withColumn(field.name, F.lit(None).cast(field.dataType))
            changes = df.withColumn("_delete", F.lit(False)).unionByName(
                delete_rows.select(*df.columns).withColumn("_delete", F.lit(True))
            )
            delete_condition = "source.`_delete`"
        elif uses_operation_delete:
            delete_condition = "source.`_operation` = 'DELETE'"

        owned_cache = not changes.is_cached
        if owned_cache:
            changes = changes.localCheckpoint(eager=True) if layer == "gold" else changes.cache()
        try:
            if changes.isEmpty():
                return
            merge = DeltaTable.forPath(self.spark, path).alias("target").merge(changes.alias("source"), join_condition)
            if delete_condition is not None:
                merge = merge.whenMatchedDelete(condition=delete_condition)
            not_delete = None if delete_condition is None else f"NOT ({delete_condition})"
            if compared:
                equality = " AND ".join(f"target.`{name}` <=> source.`{name}`" for name in compared)
                update_condition = f"NOT ({equality})"
                if not_delete is not None:
                    update_condition = f"{not_delete} AND {update_condition}"
                merge = merge.whenMatchedUpdate(condition=update_condition, set=update_values)
            if delete_keys is None:
                merge = merge.whenNotMatchedInsertAll(condition=not_delete)
            else:
                insert_values: dict[str, str | Column] = {name: f"source.`{name}`" for name in df.columns}
                merge = merge.whenNotMatchedInsert(condition="NOT source.`_delete`", values=insert_values)
            if delete_not_matched_by_source:
                merge = merge.whenNotMatchedBySourceDelete()
            merge.execute()
        finally:
            if owned_cache:
                changes.unpersist()

    def merge_scd2(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        source_key: str,
        attribute_hash: str,
        initial_effective_from: str,
        type1_columns: Sequence[str] = (),
        *,
        replace: bool = False,
    ) -> None:
        from delta.tables import DeltaTable

        path = self.config.lakehouse.table_path(layer, table_name)
        if initial_effective_from not in df.columns:
            raise ValueError(
                f"Missing initial effective date column for {layer}.{table_name}: {initial_effective_from}"
            )
        if replace or not DeltaTable.isDeltaTable(self.spark, path):
            initial = df.withColumn(
                "effective_from",
                F.when(
                    F.col(source_key).isNotNull(),
                    F.coalesce(F.col(initial_effective_from), F.col("effective_from")),
                ).otherwise(F.col("effective_from")),
            )
            self.write_table(initial, layer, table_name)
            return

        target = self.read_table(layer, table_name)
        source_columns = set(df.columns)
        target_columns = set(target.columns)
        if source_columns != target_columns:
            raise ValueError(
                f"Schema mismatch for {layer}.{table_name}; "
                f"missing_from_source={sorted(target_columns - source_columns)}, "
                f"missing_from_target={sorted(source_columns - target_columns)}. "
                "Rebuild the affected layer explicitly."
            )

        staged = _stage_scd2_changes(
            df,
            target.filter(F.col("is_current")),
            source_key=source_key,
            attribute_hash=attribute_hash,
            initial_effective_from=initial_effective_from,
            type1_columns=type1_columns,
        ).localCheckpoint(eager=True)
        delta = DeltaTable.forPath(self.spark, path)
        try:
            if staged.isEmpty():
                return
            merge = (
                delta.alias("target")
                .merge(
                    staged.alias("source"),
                    f"target.`{source_key}` = source.`_merge_key` AND target.`is_current` = true",
                )
                .whenMatchedUpdate(
                    condition="source.`_action` = 'CLOSE'",
                    set={
                        "effective_to": "source.`effective_from`",
                        "is_current": "false",
                        "updated_at": "current_timestamp()",
                    },
                )
            )
            if type1_columns:
                type1_updates: dict[str, str | Column] = {column: f"source.`{column}`" for column in type1_columns}
                type1_updates["updated_at"] = "current_timestamp()"
                merge = merge.whenMatchedUpdate(
                    condition="source.`_action` = 'TYPE1'",
                    set=type1_updates,
                )
            insert_values: dict[str, str | Column] = {column: f"source.`{column}`" for column in df.columns}
            merge.whenNotMatchedInsert(
                condition="source.`_action` = 'INSERT'",
                values=insert_values,
            ).execute()
        finally:
            staged.unpersist()


def _stage_scd2_changes(
    source: DataFrame,
    current: DataFrame,
    *,
    source_key: str,
    attribute_hash: str,
    initial_effective_from: str,
    type1_columns: Sequence[str],
) -> DataFrame:
    """Create mutually exclusive SCD2 actions consumed by one Delta MERGE."""

    current_projection = current.select(
        source_key,
        F.lit(True).alias("_target_exists"),
        F.col(attribute_hash).alias("_target_attribute_hash"),
        *(F.col(column).alias(f"_target_type1_{column}") for column in type1_columns),
    )
    classified = source.filter(F.col(source_key).isNotNull()).join(
        current_projection,
        source_key,
        "left",
    )
    is_new = F.col("_target_exists").isNull()
    type2_changed = ~is_new & ~F.col(attribute_hash).eqNullSafe(F.col("_target_attribute_hash"))
    type1_changed = F.lit(False)
    for column in type1_columns:
        type1_changed = type1_changed | ~F.col(column).eqNullSafe(F.col(f"_target_type1_{column}"))
    type1_changed = ~is_new & ~type2_changed & type1_changed
    classified = (
        classified.withColumn("_is_new", is_new)
        .withColumn("_is_type2", type2_changed)
        .withColumn("_is_type1", type1_changed)
        .withColumn(
            "_actions",
            F.when(F.col("_is_new"), F.array(F.lit("INSERT")))
            .when(F.col("_is_type2"), F.array(F.lit("CLOSE"), F.lit("INSERT")))
            .when(F.col("_is_type1"), F.array(F.lit("TYPE1")))
            .otherwise(F.array().cast("array<string>")),
        )
        .withColumn("_action", F.explode("_actions"))
    )

    payload = list(source.columns)
    merge_key_type = source.schema[source_key].dataType
    return classified.withColumn(
        "effective_from",
        F.when(
            F.col("_is_new") & (F.col("_action") == "INSERT"),
            F.coalesce(F.col(initial_effective_from), F.col("effective_from")),
        ).otherwise(F.col("effective_from")),
    ).select(
        *payload,
        F.when(
            F.col("_action") == "INSERT",
            F.lit(None).cast(merge_key_type),
        )
        .otherwise(F.col(source_key))
        .alias("_merge_key"),
        "_action",
    )


def _delta_sql_identifier(path: str) -> str:
    normalized = path if urlparse(path).scheme else Path(path).resolve().as_uri()
    return f"delta.`{normalized.replace('`', '``')}`"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _pipeline_metadata(raw: str | None, pipeline: str) -> dict[str, object] | None:
    if raw is None:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload if payload.get("pipeline") == pipeline else None
