FROM apache/airflow:2.9.3-python3.12

USER root

# Java can cho PySpark (job spark_to_silver chay ngay trong container Airflow,
# khong dung DockerOperator/cluster rieng - don gian hoa cho quy mo 1 may).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Dung toan bo toolchain cua project: crawler (curl_cffi), Spark job (pyspark),
# EL loader (duckdb), dbt (dbt-postgres). Pin pyspark=3.5.8 khop voi Java 17
# (Spark >=3.3 ho tro Java 17).
RUN pip install --no-cache-dir \
    curl_cffi \
    pandas \
    pyarrow \
    duckdb \
    dbt-core \
    dbt-postgres \
    pyspark==3.5.8 \
    requests
