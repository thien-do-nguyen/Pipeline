.DEFAULT_GOAL := help

CLOUD_ENV ?= .env.cloud
-include .env
-include $(CLOUD_ENV)

.PHONY: help setup env cloud-env pg-up pg-wait pg-down pg-reset lakehouse-reset seed seed-stream \
	run-batch-local run-batch-cloud validate-batch-cloud deploy-batch-cloud validate \
	validate-batch-local lint format format-check type-check test test-integration test-e2e check \
	smoke demo-batch-local build

VENV_PYTHON := .venv/bin/python
VENV_PIP := $(VENV_PYTHON) -m pip
VENV_BIN := .venv/bin
DATABRICKS := databricks

CONFIG ?= configs/local.yaml
SEED ?= 42
CUSTOMERS ?= 10000
ORDERS ?= 50000
SEED_BATCH_SIZE ?= 500
ORDERS_PER_BATCH ?= 5
INTERVAL_SECONDS ?= 5
MAX_BATCHES ?=
DATABRICKS_FLAGS ?=

CLOUD_REQUIRED_VARS := \
	POSTGRES_HOST \
	POSTGRES_PORT \
	POSTGRES_DB \
	POSTGRES_USER \
	DATABRICKS_PROFILE \
	DATABRICKS_TARGET \
	DATABRICKS_RUN_PRINCIPAL \
	DATABRICKS_SECRET_SCOPE \
	DATABRICKS_UC_CATALOG \
	DATABRICKS_UC_STORAGE_CREDENTIAL \
	DATABRICKS_AZURE_STORAGE_ACCOUNT \
	DATABRICKS_AZURE_STORAGE_CONTAINER \
	DATABRICKS_BRONZE_SCHEMA \
	DATABRICKS_SILVER_SCHEMA \
	DATABRICKS_GOLD_SCHEMA

DATABRICKS_BUNDLE_VARS := \
	--var="postgres_host=$(POSTGRES_HOST)" \
	--var="postgres_port=$(POSTGRES_PORT)" \
	--var="postgres_database=$(POSTGRES_DB)" \
	--var="postgres_user=$(POSTGRES_USER)" \
	--var="run_principal=$(DATABRICKS_RUN_PRINCIPAL)" \
	--var="uc_catalog=$(DATABRICKS_UC_CATALOG)" \
	--var="uc_storage_credential=$(DATABRICKS_UC_STORAGE_CREDENTIAL)" \
	--var="azure_storage_account=$(DATABRICKS_AZURE_STORAGE_ACCOUNT)" \
	--var="azure_storage_container=$(DATABRICKS_AZURE_STORAGE_CONTAINER)" \
	--var="bronze_schema=$(DATABRICKS_BRONZE_SCHEMA)" \
	--var="silver_schema=$(DATABRICKS_SILVER_SCHEMA)" \
	--var="gold_schema=$(DATABRICKS_GOLD_SCHEMA)" \
	--var="secret_scope=$(DATABRICKS_SECRET_SCOPE)"

help:
	@$(VENV_PYTHON) -c "print('Targets: setup env pg-up pg-reset seed run-batch-local run-batch-cloud validate check smoke demo-batch-local')" 2>/dev/null || \
		python3 -c "print('Targets: setup env pg-up pg-reset seed run-batch-local run-batch-cloud validate check smoke demo-batch-local')"

setup:
	python3 -m venv --copies .venv
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -e ".[dev]"

env:
	@test -f .env || cp .env.example .env

cloud-env:
	@test -f "$(CLOUD_ENV)" || { echo "Missing $(CLOUD_ENV); copy .env.cloud.example and set the cloud environment values."; exit 1; }
	@missing='$(strip $(foreach var,$(CLOUD_REQUIRED_VARS),$(if $($(var)),,$(var))))'; \
		test -z "$$missing" || { echo "Missing variables in $(CLOUD_ENV): $$missing"; exit 1; }
	@test -n "$(POSTGRES_HOST)" -a "$(POSTGRES_HOST)" != "localhost" -a "$(POSTGRES_HOST)" != "127.0.0.1" || { echo "POSTGRES_HOST in $(CLOUD_ENV) must be reachable from Databricks, not localhost."; exit 1; }

pg-up: env
	docker compose up -d postgres

pg-wait:
	docker compose exec -T postgres sh -c 'until pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"; do sleep 1; done'

pg-down:
	docker compose down

pg-reset: env
	docker compose down -v
	docker compose up -d postgres

lakehouse-reset:
	find data/lakehouse -depth -mindepth 1 ! -name .gitkeep -delete

seed: env
	$(VENV_PYTHON) -m ecommerce_pipeline.generator.cli \
		--config $(CONFIG) \
		--seed $(SEED) \
		--customers $(CUSTOMERS) \
		--orders $(ORDERS) \
		--batch-size $(SEED_BATCH_SIZE) \
		--reset

seed-stream: env
	$(VENV_PYTHON) -m ecommerce_pipeline.generator.cli \
		--config $(CONFIG) \
		--seed $(SEED) \
		--continuous \
		--orders-per-batch $(ORDERS_PER_BATCH) \
		--interval-seconds $(INTERVAL_SECONDS) \
		$(if $(MAX_BATCHES),--max-batches $(MAX_BATCHES),)

run-batch-local: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_batch --env configs/local.yaml --mode all

validate-batch-cloud: cloud-env
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle validate --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) $(DATABRICKS_BUNDLE_VARS)

deploy-batch-cloud: validate-batch-cloud
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle deploy --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) --auto-approve $(DATABRICKS_BUNDLE_VARS)

run-batch-cloud: deploy-batch-cloud
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle run --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) $(DATABRICKS_BUNDLE_VARS) ecommerce_pipeline

validate validate-batch-local: env
	$(VENV_PYTHON) -m ecommerce_pipeline.jobs.validate_batch --env $(CONFIG)

format:
	$(VENV_BIN)/ruff format src tests
	$(VENV_BIN)/ruff check src tests --fix

format-check:
	$(VENV_BIN)/ruff format src tests --check

lint:
	$(VENV_BIN)/ruff check src tests

type-check:
	MYPYPATH=src $(VENV_BIN)/mypy -p ecommerce_pipeline

test:
	$(VENV_BIN)/pytest tests/unit

test-integration: env
	RUN_INTEGRATION=1 $(VENV_BIN)/pytest tests/integration

test-e2e: env
	RUN_E2E=1 $(VENV_BIN)/pytest tests/e2e -s

check: format-check lint type-check test

smoke: pg-reset pg-wait lakehouse-reset
	$(MAKE) seed CUSTOMERS=8 ORDERS=20
	$(MAKE) run-batch-local
	$(MAKE) validate-batch-local

demo-batch-local: smoke
	$(MAKE) run-batch-local
	$(MAKE) seed-stream ORDERS_PER_BATCH=2 INTERVAL_SECONDS=0 MAX_BATCHES=1
	$(MAKE) run-batch-local
	$(MAKE) validate-batch-local

build:
	$(VENV_PYTHON) -m build --no-isolation
