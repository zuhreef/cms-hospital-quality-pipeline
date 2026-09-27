# Airflow 3 image with Java (for PySpark), the pipeline package, and dbt in an
# isolated virtualenv (dbt's pins conflict with Airflow's constraints file).
ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
# arch-independent JAVA_HOME (amd64 on Intel, arm64 on Apple Silicon)
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

# dbt in its own venv
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-duckdb>=1.10,<1.11" \
 && chown -R airflow: /opt/dbt-venv
ENV DBT_BIN=/opt/dbt-venv/bin/dbt

USER airflow
COPY --chown=airflow:root pyproject.toml /opt/project/
COPY --chown=airflow:root src /opt/project/src
COPY --chown=airflow:root config /opt/project/config
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/project \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV PROJECT_ROOT=/opt/project
WORKDIR /opt/project
