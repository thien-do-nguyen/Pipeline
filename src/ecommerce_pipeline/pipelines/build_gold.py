from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecommerce_pipeline.adapters.lakehouse import (
    LakehouseAdapter,
    delta_table_state,
    latest_delta_version,
    read_delta,
)
from ecommerce_pipeline.config.models import AppConfig
from ecommerce_pipeline.contracts.gold_tables import GOLD_TABLES, SCD2_DIMENSIONS
from ecommerce_pipeline.contracts.silver_tables import SILVER_TABLES
from ecommerce_pipeline.control.gold_releases import GoldRelease, GoldReleaseStore
from ecommerce_pipeline.control.manifests import GoldCandidateManifest, SilverBatchManifest
from ecommerce_pipeline.pipelines.quality import GoldQualityChecker
from ecommerce_pipeline.transformations.gold.dimensions import (
    build_dim_category,
    build_dim_customer,
    build_dim_date,
    build_dim_location,
    build_dim_payment,
    build_dim_product,
    build_dim_promotion,
    build_dim_shipping,
    build_dim_shop,
    build_dim_time,
)
from ecommerce_pipeline.transformations.gold.fact_sales import build_fact_sales
from ecommerce_pipeline.transformations.silver.common import SILVER_SCHEMA_VERSION

FACT_SOURCE_TABLES = {
    "orders",
    "order_items",
    "order_vouchers",
    "payments",
    "shipments",
    "vouchers",
}


class GoldBuilder:
    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        timings_ms: dict[str, int] | None = None,
        silver_manifest: SilverBatchManifest | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.lakehouse = LakehouseAdapter(spark, config)
        self.releases = GoldReleaseStore(spark, config)
        self.timings_ms = timings_ms
        self.silver_manifest = silver_manifest

    def run(self, *, batch_id: str = "standalone", full_rebuild: bool = False) -> list[str]:
        with self._timed("gold.metadata"):
            current_versions = self._current_silver_versions()
            previous_release = self.releases.latest()
        previous_versions = None if previous_release is None else previous_release.silver_versions
        if full_rebuild or previous_versions is None:
            with self._timed("gold.full_build"):
                changed_gold_tables = self._run_full()
            with self._timed("gold.publish"):
                self._publish(current_versions, batch_id, changed_gold_tables, previous_release)
            return self._paths()

        self._validate_progress(previous_versions, current_versions)
        changed_tables = {name for name, version in current_versions.items() if version > previous_versions[name]}
        if not changed_tables:
            return self._paths()

        self._validate_scd2_schemas()
        changes = {
            name: self._read_changes(name, previous_versions[name] + 1, current_versions[name])
            for name in changed_tables
        }
        owned_cache = [dataframe for dataframe in changes.values() if not dataframe.is_cached]
        for dataframe in owned_cache:
            dataframe.cache()
        try:
            with self._timed("gold.incremental"):
                changed_gold_tables = self._run_incremental(changes)
            with self._timed("gold.publish"):
                self._publish(current_versions, batch_id, changed_gold_tables, previous_release)
        finally:
            for dataframe in owned_cache:
                dataframe.unpersist()
        return self._paths()

    def _run_full(self) -> frozenset[str]:
        tables = self._read_sources()
        scd2_outputs = {
            "dim_customer": build_dim_customer(tables["app_users"], self.spark),
            "dim_shop": build_dim_shop(tables["shops"], self.spark),
            "dim_category": build_dim_category(tables["categories"], self.spark),
            "dim_product": build_dim_product(tables["products"], tables["product_variants"], self.spark),
        }
        for table_name, dimension in scd2_outputs.items():
            with self._timed(f"gold.table.{table_name}"):
                self._merge_scd2(
                    table_name,
                    dimension,
                    replace=self._scd2_requires_replacement(table_name),
                )
        fact_dimensions = self._read_fact_dimensions()
        outputs: dict[str, tuple[DataFrame, list[str]]] = {
            "dim_date": (build_dim_date(tables["orders"], self.spark), ["date_key"]),
            "dim_time": (build_dim_time(tables["orders"], self.spark), ["time_key"]),
            "dim_location": (
                build_dim_location(tables["user_addresses"], tables["orders"], self.spark),
                ["location_key"],
            ),
            "dim_promotion": (
                build_dim_promotion(tables["order_vouchers"], tables["vouchers"], self.spark),
                ["promotion_key"],
            ),
            "dim_payment": (build_dim_payment(tables["payments"], self.spark), ["payment_key"]),
            "dim_shipping": (build_dim_shipping(tables["shipments"], self.spark), ["shipping_key"]),
            "fact_sales": (
                build_fact_sales(tables, fact_dimensions),
                ["source_order_id", "source_order_item_id"],
            ),
        }
        for table_name, (df, keys) in outputs.items():
            with self._timed(f"gold.table.{table_name}"):
                self.lakehouse.upsert_table(df, "gold", table_name, keys, delete_not_matched_by_source=True)
        GoldQualityChecker(self.lakehouse).run(tables)
        return frozenset(GOLD_TABLES)

    def _run_incremental(self, changes: dict[str, DataFrame]) -> frozenset[str]:
        changed_order_ids = self._ids_if_changed(changes, "orders", "order_id")
        affected_order_ids = self._affected_order_ids(changes).localCheckpoint(eager=True)
        try:
            has_affected_orders = not affected_order_ids.isEmpty()
            return self._apply_incremental(
                changes,
                changed_order_ids,
                affected_order_ids,
                has_affected_orders=has_affected_orders,
            )
        finally:
            affected_order_ids.unpersist()

    def _apply_incremental(
        self,
        changes: dict[str, DataFrame],
        changed_order_ids: DataFrame,
        affected_order_ids: DataFrame,
        *,
        has_affected_orders: bool,
    ) -> frozenset[str]:
        dimensions_started = perf_counter()
        changed_tables = set(changes)
        changed_gold_tables: set[str] = set()
        changed_scd2: list[str] = []
        dimension_jobs: list[tuple[str, Callable[[], None]]] = []

        def add_job(table_name: str, action: Callable[[], None], *, scd2: bool = False) -> None:
            dimension_jobs.append((table_name, action))
            changed_gold_tables.add(table_name)
            if scd2:
                changed_scd2.append(table_name)

        if "app_users" in changes:

            def update_customer() -> None:
                user_ids = self._ids(changes["app_users"], "user_id")
                users = self._filter_current("app_users", "user_id", user_ids)
                self._merge_scd2("dim_customer", build_dim_customer(users, self.spark))

            add_job("dim_customer", update_customer, scd2=True)

        if "orders" in changes:
            orders = self._filter_current("orders", "order_id", changed_order_ids)

            def update_date() -> None:
                self._append_dimension_members(
                    build_dim_date(orders, self.spark),
                    "dim_date",
                    "date_key",
                )

            def update_time() -> None:
                self._append_dimension_members(
                    build_dim_time(orders, self.spark),
                    "dim_time",
                    "time_key",
                )

            add_job("dim_date", update_date)
            add_job("dim_time", update_time)
        else:
            orders = self._empty_current("orders")

        if changed_tables & {"user_addresses", "orders"}:

            def update_location() -> None:
                address_ids = self._ids_if_changed(changes, "user_addresses", "address_id")
                addresses = self._filter_current("user_addresses", "address_id", address_ids)
                self._append_dimension_members(
                    build_dim_location(addresses, orders, self.spark),
                    "dim_location",
                    "location_key",
                )

            add_job("dim_location", update_location)

        if "shops" in changes:

            def update_shop() -> None:
                shop_ids = self._ids(changes["shops"], "shop_id")
                shops = self._filter_current("shops", "shop_id", shop_ids)
                self._merge_scd2("dim_shop", build_dim_shop(shops, self.spark))

            add_job("dim_shop", update_shop, scd2=True)

        if "categories" in changes:

            def update_category() -> None:
                category_ids = self._affected_category_ids(changes["categories"])
                categories = self._category_scope(category_ids)
                self._merge_scd2("dim_category", build_dim_category(categories, self.spark))

            add_job("dim_category", update_category, scd2=True)

        if changed_tables & {"products", "product_variants"}:

            def update_product() -> None:
                product_ids = self._affected_product_ids(changes)
                products = self._filter_current("products", "product_id", product_ids)
                variants = self._filter_current("product_variants", "product_id", product_ids)
                self._merge_scd2(
                    "dim_product",
                    build_dim_product(products, variants, self.spark),
                )

            add_job("dim_product", update_product, scd2=True)

        if changed_tables & {"order_vouchers", "vouchers"}:

            def update_promotion() -> None:
                order_vouchers, vouchers = self._promotion_scope(affected_order_ids)
                promotions = build_dim_promotion(order_vouchers, vouchers, self.spark).filter("promotion_key <> 0")
                self.lakehouse.upsert_table(
                    promotions,
                    "gold",
                    "dim_promotion",
                    ["promotion_key"],
                    target_exists=True,
                )

            add_job("dim_promotion", update_promotion)

        if "payments" in changes:

            def update_payment() -> None:
                payment_ids = self._ids(changes["payments"], "payment_id")
                payments = self._filter_current("payments", "payment_id", payment_ids)
                self._append_dimension_members(
                    build_dim_payment(payments, self.spark),
                    "dim_payment",
                    "payment_key",
                )

            add_job("dim_payment", update_payment)

        if "shipments" in changes:

            def update_shipping() -> None:
                shipment_ids = self._ids(changes["shipments"], "shipment_id")
                shipments = self._filter_current("shipments", "shipment_id", shipment_ids)
                self._append_dimension_members(
                    build_dim_shipping(shipments, self.spark),
                    "dim_shipping",
                    "shipping_key",
                )

            add_job("dim_shipping", update_shipping)

        def execute_dimension(job: tuple[str, Callable[[], None]]) -> None:
            table_name, action = job
            with self._timed(f"gold.table.{table_name}"):
                action()

        workers = max(1, min(len(dimension_jobs), self.config.spark.max_parallel_tables))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(execute_dimension, dimension_jobs))

        GoldQualityChecker(self.lakehouse).validate_scd2(changed_scd2)
        self._record_elapsed("gold.dimensions", dimensions_started)

        if changed_tables & FACT_SOURCE_TABLES and has_affected_orders:
            fact_started = perf_counter()
            fact_sources = self._fact_scope(affected_order_ids)
            fact_sources["orders"] = fact_sources["orders"].cache()
            fact_sources["order_items"] = fact_sources["order_items"].cache()
            fact_dimensions = self._fact_dimension_scope(fact_sources)
            fact_keys = ["source_order_id", "source_order_item_id"]
            facts: DataFrame | None = None
            try:
                # Quality rules and Delta MERGE both consume this dataframe.
                # Cut its join/window lineage once instead of embedding that
                # large plan into every downstream aggregate and merge command.
                with self._timed("gold.fact_materialize"):
                    facts = build_fact_sales(fact_sources, fact_dimensions).localCheckpoint(eager=True)
                existing_facts = self._filter_gold(
                    "fact_sales",
                    "source_order_id",
                    affected_order_ids,
                    "order_id",
                )
                stale_fact_keys = existing_facts.select(*fact_keys).join(
                    facts.select(*fact_keys),
                    fact_keys,
                    "left_anti",
                )
                with self._timed("gold.fact_quality"):
                    GoldQualityChecker(self.lakehouse).run_incremental(
                        facts,
                        fact_sources,
                        fact_is_materialized=True,
                    )
                with self._timed("gold.fact_merge"):
                    self.lakehouse.upsert_table(
                        facts,
                        "gold",
                        "fact_sales",
                        fact_keys,
                        delete_keys=stale_fact_keys,
                    )
                    changed_gold_tables.add("fact_sales")
            finally:
                if facts is not None:
                    facts.unpersist()
                fact_sources["orders"].unpersist()
                fact_sources["order_items"].unpersist()
            self._record_elapsed("gold.fact_total", fact_started)
        return frozenset(changed_gold_tables)

    def _affected_order_ids(self, changes: dict[str, DataFrame]) -> DataFrame:
        frames = [
            self._ids(changes[name], "order_id")
            for name in ("orders", "order_items", "order_vouchers", "payments", "shipments")
            if name in changes
        ]
        if "vouchers" in changes:
            voucher_ids = self._ids(changes["vouchers"], "voucher_id")
            frames.append(self._filter_current("order_vouchers", "voucher_id", voucher_ids).select("order_id"))
            if "order_vouchers" in changes:
                frames.append(
                    changes["order_vouchers"]
                    .join(F.broadcast(voucher_ids), "voucher_id", "left_semi")
                    .select("order_id")
                )
        if not frames:
            return self._empty_current("orders").select("order_id")
        return self._union_ids(frames, "order_id")

    def _affected_category_ids(self, category_changes: DataFrame) -> DataFrame:
        changed = self._ids(category_changes, "category_id")
        children = self._filter_current("categories", "parent_category_id", changed, "category_id").select(
            "category_id"
        )
        return self._union_ids([changed, children], "category_id")

    def _category_scope(self, affected_ids: DataFrame) -> DataFrame:
        current = self.lakehouse.read_table("silver", "categories")
        affected = current.join(F.broadcast(affected_ids), "category_id", "left_semi")
        parent_ids = affected.select(F.col("parent_category_id").alias("category_id")).where("category_id IS NOT NULL")
        scope_ids = self._union_ids([affected_ids, parent_ids], "category_id")
        return current.join(F.broadcast(scope_ids), "category_id", "left_semi")

    def _affected_product_ids(self, changes: dict[str, DataFrame]) -> DataFrame:
        frames: list[DataFrame] = []
        if "products" in changes:
            frames.append(self._ids(changes["products"], "product_id"))
        if "product_variants" in changes:
            frames.append(self._ids(changes["product_variants"], "product_id"))
        return self._union_ids(frames, "product_id")

    def _promotion_scope(self, order_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
        order_vouchers = self._filter_current("order_vouchers", "order_id", order_ids)
        voucher_ids = order_vouchers.select("voucher_id").where("voucher_id IS NOT NULL").distinct()
        vouchers = self._filter_current("vouchers", "voucher_id", voucher_ids)
        return order_vouchers, vouchers

    def _fact_scope(self, order_ids: DataFrame) -> dict[str, DataFrame]:
        orders = self._filter_current("orders", "order_id", order_ids)
        items = self._filter_current("order_items", "order_id", order_ids)
        payments = self._filter_current("payments", "order_id", order_ids)
        shipments = self._filter_current("shipments", "order_id", order_ids)
        order_vouchers, vouchers = self._promotion_scope(order_ids)
        return {
            "orders": orders,
            "order_items": items,
            "payments": payments,
            "shipments": shipments,
            "order_vouchers": order_vouchers,
            "vouchers": vouchers,
        }

    def _current_silver_versions(self) -> dict[str, int]:
        if self.silver_manifest is not None:
            versions = self.silver_manifest.committed_versions
            if set(versions) != set(SILVER_TABLES):
                missing = sorted(set(SILVER_TABLES) - set(versions))
                extra = sorted(set(versions) - set(SILVER_TABLES))
                raise ValueError(f"Invalid propagated Silver versions: missing={missing}, extra={extra}")
            invalid_schemas = {
                name: result.schema_version
                for name, result in self.silver_manifest.tables.items()
                if result.schema_version != SILVER_SCHEMA_VERSION
            }
            if invalid_schemas:
                raise RuntimeError(f"Invalid propagated Silver schema versions: {invalid_schemas}")
            return versions
        return self._silver_versions()

    def _silver_versions(self) -> dict[str, int]:
        versions: dict[str, int] = {}
        for table_name in SILVER_TABLES:
            state = delta_table_state(
                self.spark,
                self.config.lakehouse.table_reference("silver", table_name),
                pipeline="bronze_to_silver",
            )
            if state.progress_version is None:
                raise RuntimeError(f"Silver progress metadata is missing for table: {table_name}")
            versions[table_name] = state.progress_version
        return versions

    @staticmethod
    def _validate_progress(previous: dict[str, int], current: dict[str, int]) -> None:
        if set(previous) != set(current):
            raise RuntimeError("Gold progress does not match Silver contracts; run with --full-rebuild-gold")
        ahead = {name: previous[name] for name in current if previous[name] > current[name]}
        if ahead:
            raise RuntimeError(f"Gold progress is ahead of Silver: {ahead}")

    def _publish(
        self,
        silver_versions: dict[str, int],
        batch_id: str,
        changed_tables: frozenset[str],
        previous_release: GoldRelease | None,
    ) -> None:
        previous_versions = {} if previous_release is None else dict(previous_release.gold_versions)
        gold_versions = dict(previous_versions)
        gold_versions.update(
            {
                table_name: latest_delta_version(
                    self.spark,
                    self.config.lakehouse.table_reference("gold", table_name),
                )
                for table_name in changed_tables
            }
        )
        missing = sorted(set(GOLD_TABLES) - set(gold_versions))
        if missing:
            raise RuntimeError(f"Gold candidate is missing committed versions: {missing}")
        candidate = GoldCandidateManifest(
            batch_id=batch_id,
            changed_tables=changed_tables,
            previous_versions=previous_versions,
            committed_versions=gold_versions,
            silver_versions=silver_versions,
            quality_status="PASSED",
        )
        self.releases.publish(candidate)

    def _read_changes(self, table_name: str, starting_version: int, ending_version: int) -> DataFrame:
        return read_delta(
            self.spark,
            self.config.lakehouse.table_reference("silver", table_name),
            options={
                "readChangeFeed": "true",
                "startingVersion": starting_version,
                "endingVersion": ending_version,
            },
        )

    def _read_sources(self) -> dict[str, DataFrame]:
        return {name: self.lakehouse.read_table("silver", name) for name in SILVER_TABLES}

    def _empty_current(self, table_name: str) -> DataFrame:
        return self.lakehouse.read_table("silver", table_name).limit(0)

    def _filter_current(
        self,
        table_name: str,
        table_column: str,
        ids: DataFrame,
        id_column: str | None = None,
    ) -> DataFrame:
        id_name = id_column or table_column
        key_set = ids.where(F.col(id_name).isNotNull()).select(F.col(id_name).alias(table_column)).distinct()
        return self.lakehouse.read_table("silver", table_name).join(
            F.broadcast(key_set),
            table_column,
            "left_semi",
        )

    def _filter_gold(
        self,
        table_name: str,
        table_column: str,
        ids: DataFrame,
        id_column: str | None = None,
    ) -> DataFrame:
        id_name = id_column or table_column
        key_set = ids.where(F.col(id_name).isNotNull()).select(F.col(id_name).alias(table_column)).distinct()
        return self.lakehouse.read_table("gold", table_name).join(
            F.broadcast(key_set),
            table_column,
            "left_semi",
        )

    @staticmethod
    def _ids(df: DataFrame, column: str) -> DataFrame:
        return df.select(column).where(F.col(column).isNotNull()).distinct()

    def _ids_if_changed(self, changes: dict[str, DataFrame], table_name: str, column: str) -> DataFrame:
        if table_name in changes:
            return self._ids(changes[table_name], column)
        return self._empty_current(table_name).select(column)

    @staticmethod
    def _union_ids(frames: Iterable[DataFrame], column: str) -> DataFrame:
        iterator = iter(frames)
        result = next(iterator).select(column)
        for frame in iterator:
            result = result.unionByName(frame.select(column))
        return result.where(F.col(column).isNotNull()).distinct()

    def _append_dimension_members(self, dimension: DataFrame, table_name: str, key: str) -> bool:
        members = dimension.filter(F.col(key) != F.lit(0))
        return self.lakehouse.append_new_rows(
            members,
            "gold",
            table_name,
            [key],
            target_exists=True,
        )

    def _merge_scd2(self, table_name: str, dimension: DataFrame, *, replace: bool = False) -> bool:
        contract = SCD2_DIMENSIONS[table_name]
        if not replace:
            dimension = dimension.filter(F.col(contract.source_key).isNotNull())
        return self.lakehouse.merge_scd2(
            dimension,
            "gold",
            table_name,
            contract.source_key,
            contract.attribute_hash,
            contract.initial_effective_from,
            type1_columns=contract.type1_columns,
            replace=replace,
        )

    def _read_fact_dimensions(self) -> dict[str, DataFrame]:
        return {name: self.lakehouse.read_table("gold", name) for name in SCD2_DIMENSIONS}

    def _fact_dimension_scope(self, fact_sources: dict[str, DataFrame]) -> dict[str, DataFrame]:
        orders = fact_sources["orders"]
        items = fact_sources["order_items"]
        customer_ids = orders.select("customer_id").where("customer_id IS NOT NULL").distinct()
        variant_ids = items.select("product_variant_id").where("product_variant_id IS NOT NULL").distinct()
        shop_ids = items.select("shop_id").where("shop_id IS NOT NULL").distinct()
        products = self._filter_gold(
            "dim_product",
            "source_product_variant_id",
            variant_ids,
            "product_variant_id",
        )
        category_ids = products.select("source_category_id").where("source_category_id IS NOT NULL").distinct()
        return {
            "dim_customer": self._filter_gold(
                "dim_customer",
                "source_customer_id",
                customer_ids,
                "customer_id",
            ),
            "dim_product": products,
            "dim_shop": self._filter_gold("dim_shop", "source_shop_id", shop_ids, "shop_id"),
            "dim_category": self._filter_gold(
                "dim_category",
                "source_category_id",
                category_ids,
            ),
        }

    def _validate_scd2_schemas(self) -> None:
        for table_name, contract in SCD2_DIMENSIONS.items():
            if not self.lakehouse.table_exists("gold", table_name):
                raise RuntimeError(f"Gold SCD2 table is missing: {table_name}. Run with --full-rebuild-gold")
            columns = set(self.lakehouse.read_table("gold", table_name).columns)
            missing = sorted(contract.required_columns - columns)
            if missing:
                raise RuntimeError(
                    f"Gold SCD2 schema is outdated for {table_name}; missing={missing}. Run with --full-rebuild-gold"
                )

    def _scd2_requires_replacement(self, table_name: str) -> bool:
        if not self.lakehouse.table_exists("gold", table_name):
            return True
        contract = SCD2_DIMENSIONS[table_name]
        columns = set(self.lakehouse.read_table("gold", table_name).columns)
        return not contract.required_columns <= columns

    def _paths(self) -> list[str]:
        return [self.config.lakehouse.table_reference("gold", name).value for name in GOLD_TABLES]

    @contextmanager
    def _timed(self, name: str) -> Iterator[None]:
        started = perf_counter()
        try:
            yield
        finally:
            self._record_elapsed(name, started)

    def _record_elapsed(self, name: str, started: float) -> None:
        if self.timings_ms is not None:
            self.timings_ms[name] = round((perf_counter() - started) * 1000)


def build_gold(
    spark: SparkSession,
    config: AppConfig,
    *,
    batch_id: str = "standalone",
    full_rebuild: bool = False,
    timings_ms: dict[str, int] | None = None,
    silver_manifest: SilverBatchManifest | None = None,
) -> list[str]:
    return GoldBuilder(spark, config, timings_ms, silver_manifest).run(
        batch_id=batch_id,
        full_rebuild=full_rebuild,
    )
