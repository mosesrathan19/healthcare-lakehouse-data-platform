# Healthcare Lakehouse Data Platform

A **synthetic-data-only** PySpark portfolio project illustrating Bronze/Silver/Gold data layers, referential integrity, quarantined records and healthcare claims analytics.

> Status: starter implementation. The 1M/10M scale options are configurations. A reproducible 1M benchmark is included, but **no performance results are claimed until it is executed**. No PHI or real medical records are included.

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

The benchmark executes the **existing generator and Bronze → Silver → Gold pipeline**, then compares 4, 8 and 16 partitions on the same one-million-claim analytics workload. Measurements include time in seconds, rows/sec, output Parquet file counts/sizes, environment metadata and complete row/amount reconciliation. It saves actual measurements locally as `benchmark_results.json`; no unmeasured performance numbers are claimed.

```bash
python -m src.benchmark --scale demo --output-dir outputs/benchmark_demo --partitions 4,8,16
python -m src.benchmark --scale medium --output-dir outputs/benchmark_1m --partitions 4,8,16
```

For Colab, start with the demo-scale smoke test, then run medium (1M encounters and 1M claims). The 10M configuration is still not benchmarked. Timings are **single-runtime Colab/laptop measurements**, not evidence of multi-node production scale.

## Validation

The pipeline checks duplicate/null primary keys, encounter/claim foreign keys, complete accepted/rejected reconciliation and Gold/Silver count consistency.

See [data model](docs/DATA_MODEL.md). A [sample Airflow DAG](airflow/dags/healthcare_batch.py) and [GitHub CI](.github/workflows/ci.yml) are included; Airflow execution requires a separately configured worker with Spark.

## Roadmap

1. **Starter in this repository:** batch synthetic data generation and Bronze/Silver/Gold Parquet
2. **Implemented demo:** simulated claims CDC, event deduplication, version-guarded upserts/deletes and replay checks
3. **Implemented demo:** Delta Lake transactional MERGE; production-grade CDC offsets, durable tombstones and multi-batch replay remain planned
4. **Planned:** production orchestration, observability, alerting
5. **Implemented benchmark code:** 1M encounter/claim performance measurement and partition comparisons; actual timed run is pending. AWS S3 and Databricks/Unity Catalog deployment remain planned

This project is a learning implementation, not a real EHR integration or HIPAA-certified deployment.
