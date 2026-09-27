.DEFAULT_GOAL := help
SHELL := /bin/bash
export DATA_DIR ?= $(CURDIR)/data
PY ?= python

help:  ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

install:  ## install the pipeline, dbt, dashboard and dev tools into the active venv
	pip install -e ".[dbt,dashboard,dev]"

run:  ## run the full pipeline on offline sample data
	hospital-pipeline run --source sample

run-api:  ## run the full pipeline against the live CMS API
	hospital-pipeline run --source api

dashboard:  ## start the Streamlit dashboard on http://localhost:8501
	streamlit run dashboard/app.py

test:  ## unit + integration tests
	pytest -v

lint:  ## ruff
	ruff check .

dbt-docs:  ## generate and serve dbt docs (lineage graph) on http://localhost:8081
	dbt docs generate --project-dir dbt --profiles-dir dbt && dbt docs serve --project-dir dbt --profiles-dir dbt --port 8081

up:  ## start Airflow + dashboard in Docker (PIPELINE_SOURCE=sample|api)
	docker compose up -d --build

down:  ## stop the Docker stack
	docker compose down

clean:  ## delete all generated data (bronze/silver/gold/warehouse) and dbt artifacts
	rm -rf data/bronze data/silver data/quarantine data/gold data/warehouse dbt/target dbt/logs

.PHONY: help install run run-api dashboard test lint dbt-docs up down clean
