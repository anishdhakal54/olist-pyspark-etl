FROM apache/airflow:3.3.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements-local.txt /requirements-local.txt

RUN pip install --no-cache-dir -r /requirements-local.txt
