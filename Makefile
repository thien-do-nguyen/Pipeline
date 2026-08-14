.DEFAULT_GOAL := help

CLOUD_ENV ?= .env.cloud
-include .env
-include $(CLOUD_ENV)

.PHONY: help setup env cloud-env pg-up pg-wait pg-down pg-reset lakehouse-reset cdc-state-reset unified-state-reset \
	cdc-up cdc-down cdc-status \
	cdc-recover-offsets \
	run-stream-local run-stream-local-once run-silver-stream-local run-silver-stream-local-once \
	run-cdc-local-once reconcile-gold-local seed seed-reset-guard seed-stream \
	run-batch-local run-batch-cloud deploy-run-batch-cloud \
	validate-batch-cloud deploy-batch-cloud validate \
	airflow-build airflow-up airflow-down airflow-status airflow-check airflow-trigger airflow-trigger-cloud airflow-logs \
	cdc-cloud-pg-bootstrap cdc-cloud-init cdc-cloud-plan cdc-cloud-apply cdc-cloud-destroy \
	deploy-cdc-cloud run-cdc-cloud cdc-cloud-start cdc-cloud-stop cdc-cloud-status cdc-cloud-logs \
	validate-batch-local lint format format-check type-check test test-integration test-e2e check \
	smoke demo-batch-local build

VENV_PYTHON := .venv/bin/python
VENV_PIP := $(VENV_PYTHON) -m pip
VENV_BIN := .venv/bin
DATABRICKS := databricks
AIRFLOW_UID ?= $(shell id -u)
AIRFLOW_COMPOSE := AIRFLOW_UID=$(AIRFLOW_UID) docker compose -f docker-compose.yaml -f infra/local/airflow/docker-compose.yaml --profile airflow
CONNECTOR_NAME ?= ecommerce-postgres-cdc
CONNECT_URL ?= http://localhost:8083

CONFIG ?= configs/local.yaml
SEED ?= 999
CUSTOMERS ?= 10000
ORDERS ?= 50000
SEED_BATCH_SIZE ?= 10000
ORDERS_PER_BATCH ?= 5
INTERVAL_SECONDS ?= 1
MAX_BATCHES ?=
DATABRICKS_FLAGS ?=
CDC_TERRAFORM_DIR := infra/cloud/cdc
AZURE_SUBSCRIPTION_ID ?= 85c4e9c5-c046-4dbe-90a1-dbdeb593fc61
AZURE_CDC_RESOURCE_GROUP ?= rg-tk1-student-cdc-dev
AZURE_CDC_NAME_PREFIX ?= tk1-ecommerce-cdc-dev

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

DATABRICKS_CDC_BUNDLE_VAR := $(if $(EVENT_HUBS_NAMESPACE),--var="event_hubs_namespace=$(EVENT_HUBS_NAMESPACE)",)

CDC_TERRAFORM_VARS := \
	-var="subscription_id=$(AZURE_SUBSCRIPTION_ID)" \
	-var="resource_group_name=$(AZURE_CDC_RESOURCE_GROUP)" \
	-var="name_prefix=$(AZURE_CDC_NAME_PREFIX)" \
	-var="postgres_host=$(POSTGRES_HOST)" \
	-var="postgres_port=$(POSTGRES_PORT)" \
	-var="postgres_database=$(POSTGRES_DB)"

help:
	@$(VENV_PYTHON) -c "print('Targets: setup env pg-up pg-reset cdc-up cdc-status cdc-recover-offsets run-stream-local run-silver-stream-local run-cdc-local-once seed run-batch-local airflow-up airflow-trigger airflow-down validate check')" 2>/dev/null || \
		python3 -c "print('Targets: setup env pg-up pg-reset cdc-up cdc-status cdc-recover-offsets run-stream-local run-silver-stream-local run-cdc-local-once seed run-batch-local airflow-up airflow-trigger airflow-down validate check')"

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
	$(MAKE) lakehouse-reset
	@test ! -d data/checkpoints || find data/checkpoints -depth -mindepth 1 -delete

lakehouse-reset:
	find data/lakehouse -depth -mindepth 1 ! -name .gitkeep -delete

cdc-state-reset: unified-state-reset
	@test ! -d data/lakehouse/bronze/cdc_events || find data/lakehouse/bronze/cdc_events -depth -delete
	@test ! -d data/lakehouse/bronze/streaming || find data/lakehouse/bronze/streaming -depth -delete
	@test ! -d data/checkpoints/ecommerce-cdc-to-bronze || \
		find data/checkpoints/ecommerce-cdc-to-bronze -depth -delete
	@test ! -d data/checkpoints/ecommerce-cdc-to-silver || \
		find data/checkpoints/ecommerce-cdc-to-silver -depth -delete

unified-state-reset:
	@test ! -d data/lakehouse/silver || find data/lakehouse/silver -depth -delete
	@test ! -d data/lakehouse/gold || find data/lakehouse/gold -depth -delete
	@test ! -d data/checkpoints/ecommerce-cdc-to-silver || \
		find data/checkpoints/ecommerce-cdc-to-silver -depth -delete

cdc-up: env
	docker compose up -d --wait postgres
	docker compose exec -T postgres bash /docker-entrypoint-initdb.d/02_cdc.sh
	docker compose --profile cdc up -d --wait connect
	docker compose --profile cdc run --rm connector-init

cdc-down:
	docker compose --profile cdc stop connect kafka

cdc-status:
	docker compose exec -T connect curl --fail --silent --show-error \
		$(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/status

cdc-recover-offsets:
	docker compose exec -T connect curl --fail-with-body --silent --show-error \
		--request PUT $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/stop
	docker compose exec -T connect curl --fail-with-body --silent --show-error \
		--request DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/offsets
	docker compose --profile cdc run --rm connector-init
	docker compose exec -T connect curl --fail-with-body --silent --show-error \
		--request PUT $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/resume
	docker compose exec -T connect curl --fail-with-body --silent --show-error \
		--request POST "$(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/restart?includeTasks=true&onlyFailed=false"

run-stream-local: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_streaming --env configs/local.yaml

run-stream-local-once: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_streaming \
		--env configs/local.yaml --available-now

run-silver-stream-local: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_silver_streaming \
		--env configs/local.yaml

run-silver-stream-local-once: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.run_silver_streaming \
		--env configs/local.yaml --available-now

run-cdc-local-once:
	$(MAKE) run-stream-local-once
	$(MAKE) run-silver-stream-local-once

reconcile-gold-local: env
	SPARK_LOCAL_IP=127.0.0.1 $(VENV_PYTHON) -m ecommerce_pipeline.jobs.reconcile_gold \
		--env configs/local.yaml

seed-reset-guard:
	@state="$$(find data/lakehouse data/checkpoints -mindepth 1 -type f ! -name .gitkeep -print -quit 2>/dev/null)"; \
		test -z "$$state" || { \
			echo "Refusing source reset while generated Lakehouse/checkpoint state exists: $$state"; \
			echo "Run 'make pg-reset' first so PostgreSQL, Kafka, Lakehouse, and checkpoints start in one source epoch."; \
			exit 1; \
		}

seed: env seed-reset-guard
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

cdc-cloud-pg-bootstrap: cloud-env
	@POSTGRES_PASSWORD='$(POSTGRES_PASSWORD)' CDC_POSTGRES_PASSWORD='$(CDC_POSTGRES_PASSWORD)' \
		$(VENV_PYTHON) -m ecommerce_pipeline.jobs.bootstrap_cloud_cdc --env-file $(CLOUD_ENV)

cdc-cloud-init: cloud-env
	terraform -chdir=$(CDC_TERRAFORM_DIR) init

cdc-cloud-plan: cdc-cloud-init
	@TF_VAR_postgres_cdc_password='$(CDC_POSTGRES_PASSWORD)' terraform -chdir=$(CDC_TERRAFORM_DIR) plan \
		$(CDC_TERRAFORM_VARS)

cdc-cloud-apply: cdc-cloud-pg-bootstrap cdc-cloud-init
	@TF_VAR_postgres_cdc_password='$(CDC_POSTGRES_PASSWORD)' terraform -chdir=$(CDC_TERRAFORM_DIR) apply \
		-auto-approve $(CDC_TERRAFORM_VARS)
	bash $(CDC_TERRAFORM_DIR)/sync_databricks_secret.sh \
		$(CDC_TERRAFORM_DIR) $(DATABRICKS_SECRET_SCOPE) $(DATABRICKS_PROFILE)
	$(MAKE) deploy-cdc-cloud

cdc-cloud-destroy: cloud-env
	-@$(MAKE) cdc-cloud-stop
	@TF_VAR_postgres_cdc_password='$(CDC_POSTGRES_PASSWORD)' terraform -chdir=$(CDC_TERRAFORM_DIR) destroy \
		-auto-approve $(CDC_TERRAFORM_VARS)

deploy-cdc-cloud: cloud-env
	@namespace="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw event_hubs_namespace)"; \
		$(MAKE) deploy-batch-cloud EVENT_HUBS_NAMESPACE="$$namespace"

run-cdc-cloud: cloud-env
	@namespace="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw event_hubs_namespace)"; \
		$(DATABRICKS) $(DATABRICKS_FLAGS) bundle run --profile $(DATABRICKS_PROFILE) \
		--target $(DATABRICKS_TARGET) $(DATABRICKS_BUNDLE_VARS) \
		--var="event_hubs_namespace=$$namespace" ecommerce_cdc

cdc-cloud-start: cloud-env
	@namespace="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw event_hubs_namespace)"; \
		job_id="$$($(DATABRICKS) bundle summary --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) \
		$(DATABRICKS_BUNDLE_VARS) --var="event_hubs_namespace=$$namespace" -o json | \
		$(VENV_PYTHON) -c 'import json,sys; print(json.load(sys.stdin)["resources"]["jobs"]["ecommerce_cdc"]["id"])')"; \
		$(DATABRICKS) jobs update "$$job_id" --profile $(DATABRICKS_PROFILE) --json \
		'{"new_settings":{"schedule":{"quartz_cron_expression":"0 0/2 * * * ?","timezone_id":"Asia/Ho_Chi_Minh","pause_status":"UNPAUSED"}}}'; \
		printf '[cloud-cdc] schedule=UNPAUSED interval=2m job_id=%s\n' "$$job_id"

cdc-cloud-stop: cloud-env
	@namespace="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw event_hubs_namespace)"; \
		job_id="$$($(DATABRICKS) bundle summary --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) \
		$(DATABRICKS_BUNDLE_VARS) --var="event_hubs_namespace=$$namespace" -o json | \
		$(VENV_PYTHON) -c 'import json,sys; print(json.load(sys.stdin)["resources"]["jobs"]["ecommerce_cdc"]["id"])')"; \
		$(DATABRICKS) jobs update "$$job_id" --profile $(DATABRICKS_PROFILE) --json \
		'{"new_settings":{"schedule":{"quartz_cron_expression":"0 0/2 * * * ?","timezone_id":"Asia/Ho_Chi_Minh","pause_status":"PAUSED"}}}'; \
		printf '[cloud-cdc] schedule=PAUSED job_id=%s\n' "$$job_id"

cdc-cloud-status: cloud-env
	@terraform -chdir=$(CDC_TERRAFORM_DIR) output
	@container="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw debezium_container_group)"; \
		az container show --resource-group $(AZURE_CDC_RESOURCE_GROUP) --name "$$container" \
		--query '{name:name,state:instanceView.state,containers:containers[].{name:name,state:instanceView.currentState.state,restarts:instanceView.restartCount}}' --output json

cdc-cloud-logs: cloud-env
	@container="$$(terraform -chdir=$(CDC_TERRAFORM_DIR) output -raw debezium_container_group)"; \
		az container logs --resource-group $(AZURE_CDC_RESOURCE_GROUP) --name "$$container" \
			--container-name debezium-server

airflow-build: env
	$(AIRFLOW_COMPOSE) build airflow-init

airflow-up: env
	$(AIRFLOW_COMPOSE) up -d --wait postgres airflow-api-server airflow-scheduler airflow-dag-processor

airflow-down:
	$(AIRFLOW_COMPOSE) stop airflow-api-server airflow-scheduler airflow-dag-processor airflow-db
	$(AIRFLOW_COMPOSE) rm -f airflow-api-server airflow-scheduler airflow-dag-processor airflow-init airflow-db

airflow-status:
	$(AIRFLOW_COMPOSE) ps

airflow-check:
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags list-import-errors

airflow-trigger:
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags trigger ecommerce_batch_local

airflow-trigger-cloud:
	$(AIRFLOW_COMPOSE) exec -T airflow-scheduler airflow dags trigger ecommerce_databricks_batch_cloud

airflow-logs:
	$(AIRFLOW_COMPOSE) logs --tail=200 airflow-scheduler airflow-dag-processor

validate-batch-cloud: cloud-env
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle validate --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) $(DATABRICKS_BUNDLE_VARS) $(DATABRICKS_CDC_BUNDLE_VAR)

deploy-batch-cloud: validate-batch-cloud
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle deploy --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) --auto-approve $(DATABRICKS_BUNDLE_VARS) $(DATABRICKS_CDC_BUNDLE_VAR)

# Data execution and code deployment are deliberately separate. Re-deploying on
# every scheduled run creates a new dynamic wheel and makes shared compute spend
# time reconciling historical task libraries.
run-batch-cloud: cloud-env
	$(DATABRICKS) $(DATABRICKS_FLAGS) bundle run --profile $(DATABRICKS_PROFILE) --target $(DATABRICKS_TARGET) $(DATABRICKS_BUNDLE_VARS) $(DATABRICKS_CDC_BUNDLE_VAR) ecommerce_pipeline

# Explicit CI/CD or developer command used only when code/config/job definition changed.
deploy-run-batch-cloud: deploy-batch-cloud run-batch-cloud

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
