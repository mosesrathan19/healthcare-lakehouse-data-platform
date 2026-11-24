# Phase 4 — Advanced Data Quality, Monitoring and Airflow

Phase 4 extends the existing **synthetic** healthcare lakehouse without changing the completed Phase 1–3 implementations. It implements independent **quality gates** and a development **Airflow 3 orchestration** path.

## 1. Design

```text
Synthetic Bronze ingestion (patients/providers/encounters/claims)
                ↓
Bronze → Silver + rejected rows → Gold reporting
                ↓
QUALITY GATE: schema, keys, FKs, amounts, statuses, dates,
              rejection limits, counts, Gold reconciliation
                ↓
PASS: record metrics in outputs/.../monitoring/
FAIL: write a failure report and fail the Airflow task
```

`src/data_quality.py` reads published Parquet and validates: required columns; null/duplicate keys; encounter-to-patient/provider references; claim-to-encounter references; nonnegative billed cents; allowed PAID/DENIED statuses; valid dates; rejected-row percentage; expected dataset sizes; and Silver-to-Gold claim/paid-amount reconciliation. Quality checks return structured `PASS`/`FAIL` JSON with individual findings.

`src/phase4_cli.py` exposes three separate stages: `generate`, `build`, `validate`. Each stage records UTC start/end, runtime, Spark/Python version, outcome, and result in `outputs/<run>/monitoring/<stage>_latest.json`. Validation saves `quality_report.json` even when checks fail, and exits nonzero. This allows Airflow to retry/fail and capture logs.

These are **local JSON monitoring artifacts**, not a production metrics backend, notification system, HIPAA audit log, or distributed scheduler.

## 2. Google Colab instructions (recommended first)

[**Launch guided Phase 4 Colab notebook**](https://colab.research.google.com/github/mosesrathan19/healthcare-lakehouse-data-platform/blob/main/notebooks/04_data_quality_colab.ipynb)

1. Install compatible PySpark and restart the session if prompted.
2. Clone/pull the repository.
3. Generate the 10K synthetic demo.
4. Build Silver/Gold.
5. Run validation gates and view structured JSON checks.
6. Inject one invalid claim **in-memory only**, and confirm the result is `FAIL`.
7. Inspect monitoring reports and optionally execute `tests/test_data_quality.py`.

CLI alternative (from repo root with PySpark installed):

```bash
python -m src.phase4_cli --stage generate --scale demo --output-dir outputs/phase4_demo
python -m src.phase4_cli --stage build --scale demo --output-dir outputs/phase4_demo
python -m src.phase4_cli --stage validate --scale demo --output-dir outputs/phase4_demo
```

A `--max-reject-fraction` option permits a controlled fraction of quarantined rows; default is **0**, appropriate for our deterministic clean demo. A nonzero exception threshold should be justified by real data-quality SLAs rather than used to mask errors.

## 3. MacBook Airflow installation

**Prerequisites:** Docker Desktop running, `docker compose version` working, at least ~6–8 GB RAM available if possible. Airflow and PySpark share a container, so the first build downloads Java and Python dependencies. This environment is for **local demonstration**, not production.

Copy/paste **one Terminal command at a time** (no Markdown brackets around the Git URL):

```bash
cd ~/Documents
git clone https://github.com/mosesrathan19/healthcare-lakehouse-data-platform.git
```

If the folder already exists, instead:

```bash
cd ~/Documents/healthcare-lakehouse-data-platform
git pull --ff-only
```

Then:

```bash
cd ~/Documents/healthcare-lakehouse-data-platform/airflow
docker ps
docker compose up --build -d
docker compose ps -a
docker compose logs --tail=100 airflow
```

If `docker ps` hangs, press **Control+C**, open Docker Desktop, and check that the engine is running before continuing. Do not repeatedly reinstall Docker without checking its status.

Open **http://localhost:8080**, once the API server is listening. `airflow standalone` starts the development services and generates credentials on first run. The admin login information is in Airflow's initialization output/container logs or its auth-manager password file (depending on image configuration). Never publish credentials to GitHub.

Find DAG **`healthcare_phase4_quality_pipeline`**, and manually **Trigger DAG**. It has three separate tasks:

1. `generate_synthetic_bronze`
2. `build_silver_and_gold`
3. `run_quality_gates`

Click a task to inspect **logs, status, duration and retries**. Successful output is under `outputs/phase4_airflow_demo/monitoring/` in the repository. The DAG uses one retry per task and prevents overlapping runs.

Shut down when finished:

```bash
docker compose down
```

This preserves the named Airflow metadata volume. **Do not use** `docker compose down -v` unless intentionally deleting Airflow's local state.

## 4. Scheduling and monitoring limitations

The DAG is **manually triggered (`schedule=None`)**. The input data is fixed and synthetic, so blindly scheduling it daily would simply regenerate the same dataset. To make recurring scheduling meaningful, introduce new dated source batches, durable per-batch identifiers, checkpointed ingestion, and isolated run outputs. The current pipeline uses full-refresh Parquet outputs, not transactional multi-writer Delta commits. The monitoring JSONs overwrite the latest per-stage file on a retry and are not an immutable audit history.

For production, add object storage, service identities, alert routing, task-level metrics, run IDs, structured event storage, and managed Airflow or a supported database/worker configuration. Do **not** use real PHI with this demo deployment.

## 5. Test and validation status

Test file: `tests/test_data_quality.py`. Cases cover clean data, negative amounts, orphan claims, duplicate IDs, report writes, and failing expected-count thresholds. Run:

```bash
pytest -q tests/test_data_quality.py
```

The source files have been committed and verified on GitHub. **Execution in Colab and Docker still needs to be verified by running them.**
