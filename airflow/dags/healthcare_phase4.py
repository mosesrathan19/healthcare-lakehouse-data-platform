"""Phase 4 local Airflow 3 DAG — synthetic healthcare lakehouse.

Runs deliberately on demand, because the input data is fixed and synthetic.
Do not enable a daily schedule until using date-scoped inputs and run IDs.
"""
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

ROOT = "/opt/project"
OUTPUT = "outputs/phase4_airflow_demo"
COMMON = f"cd {ROOT} && python -m src.phase4_cli"

@dag(
    dag_id="healthcare_phase4_quality_pipeline",
    description="Generate synthetic data, build lakehouse, enforce quality gates",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["healthcare", "pyspark", "data-quality", "phase4"],
)
def healthcare_phase4():
    generate = BashOperator(
        task_id="generate_synthetic_bronze",
        bash_command=f"{COMMON} --stage generate --scale demo --output-dir {OUTPUT}",
        append_env=True,
        env={"SPARK_LOCAL_IP": "127.0.0.1"},
    )
    build = BashOperator(
        task_id="build_silver_and_gold",
        bash_command=f"{COMMON} --stage build --scale demo --output-dir {OUTPUT}",
        append_env=True,
        env={"SPARK_LOCAL_IP": "127.0.0.1"},
    )
    check = BashOperator(
        task_id="run_quality_gates",
        bash_command=f"{COMMON} --stage validate --scale demo --output-dir {OUTPUT}",
        append_env=True,
        env={"SPARK_LOCAL_IP": "127.0.0.1"},
    )
    generate >> build >> check

healthcare_phase4()
