# Local educational Airflow runner, not production infrastructure.
FROM apache/airflow:3.3.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "pyspark==3.5.10"
ENV PYTHONPATH=/opt/project
ENV SPARK_LOCAL_IP=127.0.0.1
