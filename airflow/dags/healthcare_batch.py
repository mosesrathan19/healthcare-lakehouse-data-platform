"""Airflow 3 example DAG; requires Spark and project mounted in workers."""
from datetime import datetime, timezone
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="healthcare_lakehouse_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["healthcare", "pyspark", "lakehouse"],
)
def healthcare_lakehouse_demo():
    BashOperator(
        task_id="generate_validate_publish",
        bash_command="cd /opt/project && python -m src.cli --scale demo --output-dir outputs/airflow",
    )

healthcare_lakehouse_demo()
