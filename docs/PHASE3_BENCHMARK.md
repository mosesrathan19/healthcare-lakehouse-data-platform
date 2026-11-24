# Phase 3 — Reproducible 1-million-record PySpark benchmark

**[Launch the guided Google Colab notebook](https://colab.research.google.com/github/mosesrathan19/healthcare-lakehouse-data-platform/blob/main/notebooks/03_one_million_benchmark_colab.ipynb)**

## Workload and scale

The **medium** configuration creates:

| Source dataset | Synthetic rows |
|---|---:|
| Patients | 100,000 |
| Providers | 2,000 |
| Encounters | 1,000,000 |
| Claims | 1,000,000 |

Generation uses Spark `range()`; no CSV download and no patient records or PHI. Data is deterministic. The benchmark runs the **actual project** `generate()` and `build()` functions with a fresh output directory.

## Timed phases

1. Generate and write Bronze Parquet datasets.
2. Execute Bronze → validated Silver → Gold pipeline, including quality checks and writes.
3. Load the same validated Silver claims.
4. Test `repartition(N, "service_date")` for **N = 4, 8, 16**, then group by service date, sum claims and billed cents, write Parquet, read it back, and validate totals.
5. Store the measured results in `outputs/benchmark_1m/benchmark_results.json` (excluded from Git).

The timed partition trials include read/write and reconciliation, so they are **end-to-end analytics trial timings**, not pure aggregation benchmarks. They are executed in sequence with the same shuffle settings; JVM warm-up, local OS cache, variable Colab hardware, and storage conditions can affect comparisons. They should be repeated in randomized order before drawing performance conclusions.

## Run in Colab

Use a fresh Colab runtime for the benchmark and open the notebook link above. It includes installation, GitHub clone/pull, a small smoke test, the 1M run, a comparison table, a chart and a report download.

## Run locally

```bash
pip install -r requirements.txt
python -m src.benchmark --scale demo --output-dir outputs/benchmark_demo --partitions 2,4
python -m src.benchmark --scale medium --output-dir outputs/benchmark_1m --partitions 4,8,16
```

Use Java 17 and sufficient free RAM/disk. The medium run could be resource-intensive on laptop/Colab. It does **not** require Docker, Airflow, Delta, or AWS.

## Output JSON

Key fields: `environment` (Spark/Python/CPU), `source_counts`, `quality`, `timings_seconds`, `storage`, and `partition_comparison.trials`. Each trial records `seconds`, `input_rows`, `grouped_rows`, `billed_cents`, `rows_per_second`, `output_parquet_files`, `output_bytes`, and `reconciled`.

Quality gates require **exactly 1,000,000 accepted encounters and claims**, zero rejected rows for the deliberately clean source, and identical claims counts and billed amounts for all partition trials. If a gate fails, the run stops instead of publishing misleading results.

## Results status

No performance results are supplied in this repository in advance. **Run the benchmark first**, save its JSON file, and report the actual values plus environment. Do not claim a 1M-record production benchmark merely because the scale is configurable.

## Portfolio talking point (fill in after execution)

> Built a synthetic healthcare data lakehouse with PySpark, generated 1 million encounters and 1 million claims, validated Bronze/Silver/Gold reconciliations, and benchmarked 4/8/16 partition analytics strategies on [Colab runtime type] with observed times of [measured seconds]. All data is synthetic; measurements reflect single-runtime local Spark.
