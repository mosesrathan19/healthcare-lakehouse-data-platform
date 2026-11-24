# Healthcare Lakehouse Data Platform

A **synthetic-data-only** PySpark portfolio project illustrating Bronze/Silver/Gold data layers, referential integrity, quarantined records and healthcare claims analytics.

> Status: starter implementation. The 1M/10M scale options are configurations, **not benchmarks or claims of verified execution**. No PHI or real medical records are included.

## Architecture

```text
Spark-generated synthetic patients / providers / encounters / claims
       → Bronze Parquet
       → Silver validated Parquet + quarantine
       → Gold claims by specialty / encounter day
       → Quality reconciliation report
```

## Configured scales

| Scale | Patients | Providers | Encounters | Claims |
|---|---:|---:|---:|---:|
| demo | 1,000 | 100 | 10,000 | 10,000 |
| medium | 100,000 | 2,000 | 1,000,000 | 1,000,000 |
| large | 1,000,000 | 10,000 | 10,000,000 | 10,000,000 |

Start with demo. The large profile requires appropriate hardware and has not been benchmarked.

## Run locally

Python 3.11 and Java 17 recommended.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.cli --scale demo --output-dir outputs/demo
pytest -q
```

## Run in Google Colab

```python
%pip -q install "pyspark>=3.5,<4"
!git clone https://github.com/mosesrathan19/healthcare-lakehouse-data-platform.git
%cd healthcare-lakehouse-data-platform
!python -m src.cli --scale demo --output-dir outputs/demo
```

## Validation

The pipeline checks duplicate/null primary keys, encounter/claim foreign keys, complete accepted/rejected reconciliation and Gold/Silver count consistency.

See [data model](docs/DATA_MODEL.md). A [sample Airflow DAG](airflow/dags/healthcare_batch.py) and [GitHub CI](.github/workflows/ci.yml) are included; Airflow execution requires a separately configured worker with Spark.

## Roadmap

1. **Starter in this repository:** batch synthetic data generation and Bronze/Silver/Gold Parquet
2. **Planned:** CDC, late-arriving data, incremental state and replay
3. **Planned:** Delta Lake or Iceberg MERGE transactions
4. **Planned:** production orchestration, observability, alerting
5. **Planned:** AWS S3, Databricks/Unity Catalog, performance benchmarks

This project is a learning implementation, not a real EHR integration or HIPAA-certified deployment.
