# Healthcare Lakehouse Data Platform

A **synthetic-data-only** PySpark portfolio project illustrating Bronze/Silver/Gold data layers, referential integrity, quarantined records and healthcare claims analytics.

> Status: starter implementation. The 1M/10M scale options are configurations. The **1M benchmark has been executed on Google Colab**, with measured results documented below; 10M remains unbenchmarked. No PHI or real medical records are included.

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

## Phase 2 — Simulated CDC and Delta Lake MERGE

**[Run Phase 2 in Google Colab](https://colab.research.google.com/github/mosesrathan19/healthcare-lakehouse-data-platform/blob/main/notebooks/02_delta_cdc_colab.ipynb)**

This phase builds on the original **10K-claim demo** and demonstrates a version-aware, transactional Delta MERGE against the healthcare claims table.

| CDC operation | Events | Expected outcome |
|---|---:|---|
| Insert new claims | 100 | +100 claims |
| Update existing claims | 20 | Change billed values |
| Delete existing claims | 5 | −5 claims |
| Older duplicate updates | 5 | Discard older versions |
| **All events** | **130** | **10,095 current claims** |

**What's implemented:** deterministic fabricated CDC events from immutable Bronze data, latest-event selection, rejection of same-key/same-timestamp collisions, Delta MERGE, event-version guards, replay checks, foreign-key validation, and refreshed Gold claims-by-status Parquet reporting. Original batch Parquet layers are unchanged.

**Run locally:** install Delta separately with `pip install "delta-spark>=3.3,<3.4"` (requires compatible Spark 3.5.x), start a **Delta-enabled SparkSession** following the notebook, run the original demo and then `src.delta_cdc.run(spark, output_dir)`. Run event tests with `pytest -q tests/test_delta_cdc.py`; opt in to the slower end-to-end Delta test with `RUN_DELTA_INTEGRATION=1 pytest -q tests/test_delta_cdc.py`. For Colab, use a fresh session before starting Delta.

**Limitations:** The feed is simulated rather than streamed from a live EHR or claims database. The state table stores a per-row version for existing claims, but delete tombstones and source offsets are **not persisted across batches**; a future older insert can therefore resurrect a deleted claim if incorrectly ingested. The demo is sequential and requires additional controls for concurrent writers, source delivery guarantees, auditability, schema evolution and production compliance.

## Phase 3 — Measured 1M Healthcare Records Benchmark

**[Run the Phase 3 Colab notebook](https://colab.research.google.com/github/mosesrathan19/healthcare-lakehouse-data-platform/blob/main/notebooks/03_one_million_benchmark_colab.ipynb)** · [Full benchmark methodology](docs/PHASE3_BENCHMARK.md)

The benchmark executes the **existing generator and Bronze → Silver → Gold pipeline**, then compares 4, 8 and 16 partitions on the same one-million-claim analytics workload. Measurements include time in seconds, rows/sec, output Parquet file counts/sizes, environment metadata and complete row/amount reconciliation. It saves actual measurements locally as `benchmark_results.json`; measured results from one Colab run are documented below; repeat trials for stronger performance conclusions.

```bash
python -m src.benchmark --scale demo --output-dir outputs/benchmark_demo --partitions 4,8,16
python -m src.benchmark --scale medium --output-dir outputs/benchmark_1m --partitions 4,8,16
```

For Colab, start with the demo-scale smoke test, then run medium (1M encounters and 1M claims). The 10M configuration is still not benchmarked. Timings are **single-runtime Colab/laptop measurements**, not evidence of multi-node production scale.

## Verified Phase 3 benchmark results — November 24, 2026

The **medium 1M benchmark has now been executed on Google Colab** (Spark 3.5.10, Python 3.13.18, 2 CPUs, local Spark).

- **1,000,000 encounters and 1,000,000 claims** processed with **0 rejected** records.
- **22.739 seconds** for synthetic data generation and **64.555 seconds** for Bronze → Silver → Gold processing; **87.294 seconds total** (excluding environment setup).
- Partition experiment (same 1M-claim aggregation and Parquet read/write/reconciliation): **4 partitions: 1.873 s**, **8 partitions: 1.675 s**, **16 partitions: 2.281 s**.
- **8 partitions were fastest in this single run**; all trials reconciled exactly 1M claims and billed amounts. Results are not evidence of multi-node scaling.

**[Read the full measured results and methodology](docs/PHASE3_MEASURED_RESULTS_2026-11-24.md)**.

## Phase 4 — Advanced Data Quality + Airflow Orchestration

**[Open the Phase 4 Google Colab notebook](https://colab.research.google.com/github/mosesrathan19/healthcare-lakehouse-data-platform/blob/main/notebooks/04_data_quality_colab.ipynb)** · **[MacBook Airflow setup and full runbook](docs/PHASE4_QUALITY_AIRFLOW.md)**

This phase adds independent, **fail-closed** data-quality checks and an Airflow 3 DAG. The quality framework validates required schemas, null/duplicate primary keys, cross-table foreign keys, claim amounts and statuses, dates, accepted/rejected reconciliation, rejection thresholds, and Gold-vs-Silver totals. Every stage emits a structured JSON status/duration report. A failed gate makes the validation task fail.

- **Colab:** run the notebook to generate synthetic Bronze, build Silver/Gold, inspect monitoring reports, and inject invalid test data to demonstrate a failure.
- **MacBook:** use `airflow/compose.yaml` and `airflow/Dockerfile` with Docker Desktop, then manually trigger the DAG `healthcare_phase4_quality_pipeline` at `http://localhost:8080`.
- **Tests:** `pytest -q tests/test_data_quality.py` includes passing and intentionally failing cases.

The DAG is intentionally manual because the synthetic source is static. The three-task Airflow pipeline has been executed successfully on the user's MacBook; cloud alerting and durable remote metrics remain **not deployed**.

## Phase 5 — AWS S3 + Databricks Cloud Lakehouse (opt-in code)

**[Phase 5 deployment runbook and cost precautions](docs/PHASE5_CLOUD_DEPLOYMENT.md)** · **[Databricks managed Delta notebook](notebooks/05_databricks_managed_delta.ipynb)**

Phase 5 adds two deliberately separate cloud paths:

- **Databricks managed Delta:** `src/cloud_pipeline.py` generates synthetic data and creates nine Unity Catalog **managed Delta** Bronze/Silver/Gold tables, with claim-count and billed-amount reconciliation. The notebook starts with `EXECUTE_CLOUD_WRITE=False`, so preview mode performs no writes. A 10K-claim demo is recommended before optional 1M.
- **AWS S3:** `src/s3_upload.py` previews the local Parquet files to be uploaded to a **preexisting private bucket**, without credentials or network calls. Upload requires `--execute`, and the default size cap is 100 MiB. S3 storage/requests/transfers may be charged, and connection from Databricks to S3 additionally requires an IAM role and Unity Catalog external location.
- **Tests:** `pytest -q tests/test_phase5_cloud.py` covers cloud naming, scale limitations, dry-run behavior, upload opt-in with mocked client, and unsafe/oversized S3 manifests.

**Execution status:** Code and guidance committed; neither AWS resources nor paid Databricks jobs have been provisioned, and live cloud execution remains to be validated by the user. Databricks Free Edition is the recommended first step; see the runbook for cost controls, limitations and cleanup.

## Validation

The pipeline checks duplicate/null primary keys, encounter/claim foreign keys, complete accepted/rejected reconciliation and Gold/Silver count consistency.

See [data model](docs/DATA_MODEL.md). A [sample Airflow DAG](airflow/dags/healthcare_batch.py) and [GitHub CI](.github/workflows/ci.yml) are included; Airflow execution requires a separately configured worker with Spark.

## Roadmap

1. **Starter in this repository:** batch synthetic data generation and Bronze/Silver/Gold Parquet
2. **Implemented demo:** simulated claims CDC, event deduplication, version-guarded upserts/deletes and replay checks
3. **Implemented demo:** Delta Lake transactional MERGE; production-grade CDC offsets, durable tombstones and multi-batch replay remain planned
4. **Executed Phase 4 locally:** reusable quality gates, structured JSON monitoring, three-task Airflow DAG; production monitoring remains planned
5. **Executed on Colab:** 1M encounter/claim performance measurement and partition comparisons; repeat trials and 10M remain pending. Phase 5 S3/Databricks implementation is committed; opt-in cloud execution remains pending

This project is a learning implementation, not a real EHR integration or HIPAA-certified deployment.
