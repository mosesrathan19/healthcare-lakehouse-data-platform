# Measured Phase 3 benchmark — 1 million healthcare claims

**Run timestamp (UTC):** 2026-11-24T17:46:21.918419+00:00  
**Source:** User-executed Google Colab benchmark, `benchmark_results.json`  
**Scope:** Synthetic healthcare records, single-machine local Spark; no real patient data.

## Dataset and quality

| Dataset | Source records |
|---|---:|
| Patients | 100,000 |
| Providers | 2,000 |
| Encounters | 1,000,000 |
| Claims | 1,000,000 |

All 1,000,000 encounters and all 1,000,000 claims were accepted. **0 rejected encounters, 0 rejected claims.** The claims totals and billed cents reconciled for every partition trial.

## Environment

| Field | Measured value |
|---|---|
| Python | 3.13.18 |
| Spark / PySpark | 3.5.10 |
| Environment | Linux x86_64 / Google Colab |
| Available CPUs | 2 |
| Spark master | `local[*]` |
| Spark shuffle partitions | 16 |
| Adaptive Query Execution | Disabled |
| Initial input claim partitions | 2 |

## End-to-end pipeline timings

| Component | Measured time |
|---|---:|
| Generate synthetic Bronze data | **22.739 s** |
| Bronze → Silver → Gold pipeline | **64.555 s** |
| **Total (excluding pip and Spark startup)** | **87.294 s** |

Total Parquet storage reported: Bronze 8 files / 22,382,978 bytes; Silver 10 files / 22,390,147 bytes; Gold 20 files / 26,172 bytes. These are generated datasets, not the partition-trial outputs.

## Partition comparison (one trial per setting)

Workload: Read the same Silver claims, repartition on `service_date`, group by `service_date`, sum claims and billed cents, write Parquet, read it back, and reconcile. All trials produced **365 grouped rows**, **1,000,000 source claims**, and **109,975,000,000 billed cents**.

| Repartition count | Duration (s) | Rows/s | Parquet files | Output bytes | Reconciled |
|---:|---:|---:|---:|---:|---|
| 4 | 1.873 | 533,902.83 | 4 | 7,760 | Yes |
| **8** | **1.675** | **597,014.93** | **8** | **12,120** | **Yes** |
| 16 | 2.281 | 438,404.21 | 16 | 20,838 | Yes |

Eight partitions were fastest **in this single sequential run**: about 10.6% shorter elapsed time than four partitions, and 26.6% shorter than sixteen. This does **not** prove eight partitions are optimal on other runtimes or workloads.

## Reproducibility

See `src/benchmark.py`, `notebooks/03_one_million_benchmark_colab.ipynb`, and `docs/PHASE3_BENCHMARK.md`.

```bash
python -m src.benchmark --scale medium --output-dir outputs/benchmark_1m --partitions 4,8,16
```

Source generation and the full pipeline are each timed separately. Partition trials include aggregation, Parquet output, reading results, and reconciliation. Timings exclude Spark startup and environment installation.

## Interpretation and limitations

- The workload uses **synthetic** records and runs Spark in **local mode on two CPUs**, not a distributed multi-node environment.
- Comparisons were each performed **once in fixed order** (4, 8, 16). JVM warm-up, caching, background load, and Colab hardware variability may influence measured differences.
- Repeating experiments, varying trial order, and adding shuffle/task-level metrics would provide stronger tuning evidence.
- The `rows_per_second` metric divides the 1M input-claim count by each partition-trial duration; it is **not** full pipeline throughput.
- These results do not establish production readiness, HIPAA compliance, or operational performance with live healthcare data.
