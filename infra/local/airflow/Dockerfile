ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/spark-jars \
    && curl --fail --location --retry 3 \
        --output /opt/airflow/spark-jars/delta-spark_2.12-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl --fail --location --retry 3 \
        --output /opt/airflow/spark-jars/delta-storage-3.2.0.jar \
        https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl --fail --location --retry 3 \
        --output /opt/airflow/spark-jars/antlr4-runtime-4.9.3.jar \
        https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
    && curl --fail --location --retry 3 \
        --output /opt/airflow/spark-jars/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && echo "51d473537d1bc10c81f48b03d8e2a6b604e1b421a70835ec12e917a4245a31d5  /opt/airflow/spark-jars/delta-spark_2.12-3.2.0.jar" | sha256sum --check - \
    && echo "58aab63eba7736fea9e03eafb0dde6704a34a70f570c1a69ab8e4012c25a95d4  /opt/airflow/spark-jars/delta-storage-3.2.0.jar" | sha256sum --check - \
    && echo "131a6594969bc4f321d652ea2a33bc0e378ca312685ef87791b2c60b29d01ea5  /opt/airflow/spark-jars/antlr4-runtime-4.9.3.jar" | sha256sum --check - \
    && echo "a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268  /opt/airflow/spark-jars/postgresql-42.7.3.jar" | sha256sum --check -

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src

# Keep Airflow pinned while installing the local Spark/Delta runtime.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-databricks==7.18.0" \
    ".[local]"
