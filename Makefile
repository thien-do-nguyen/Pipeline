.DEFAULT_GOAL := help

.PHONY: help setup env pg-up pg-wait pg-down pg-reset lakehouse-reset seed seed-stream \
	run-batch-local validate validate-batch-local lint format format-check type-check test \
	test-integration test-e2e check smoke demo-batch-local build

VENV_PYTHON := .venv/bin/python
VENV_PIP := $(VENV_PYTHON) -m pip
VENV_BIN := .venv/bin

CONFIG ?= configs/local.yaml
SEED ?= 42
CUSTOMERS ?= 100
ORDERS ?= 500
SEED_BATCH_SIZE ?= 100
ORDERS_PER_BATCH ?= 5
INTERVAL_SECONDS ?= 5
MAX_BATCHES ?=

help:
	@$(VENV_PYTHON) -c "print('Targets: setup env pg-up pg-reset seed run-batch-local validate check smoke demo-batch-local')" 2>/dev/null || \
		python3 -c "print('Targets: setup env pg-up pg-reset seed run-batch-local validate check smoke demo-batch-local')"

setup:
	python3 -m venv --copies .venv
	$(VENV_PIP) install --upgrade pip
	$(VENV_PIP) install -e ".[dev]"

env:
	@test -f .env || cp .env.example .env

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
	$(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_batch --env configs/local.yaml --mode all

run-batch-cloud:
	$(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_batch --env configs/azure.yaml --mode all

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
	$(VENV_PYTHON) -m build
