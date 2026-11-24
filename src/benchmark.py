"""Reproducible PySpark healthcare benchmark.

Measures end-to-end source generation, lakehouse build, and partition-strategy
analytics (read -> repartition -> groupBy -> write Parquet -> verify).
All input data is synthetic. No benchmark durations are hardcoded.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import time

from pyspark.sql import SparkSession, functions as F
from src.config import SCALES
from src.generate import generate
from src.pipeline import build


def validate_partition_settings(text: str) -> list[int]:
    values = [int(part.strip()) for part in text.split(",") if part.strip()]
    if not values or len(values) != len(set(values)) or any(v < 1 or v > 256 for v in values):
        raise ValueError("Use unique partition numbers between 1 and 256, e.g. 4,8,16")
    return values


def count_parquet_files(path: Path) -> tuple[int, int]:
    files = list(path.rglob("*.parquet"))
    return len(files), sum(p.stat().st_size for p in files)


def elapsed(action):
    t0 = time.perf_counter()
    result = action()
    return result, round(time.perf_counter() - t0, 3)


def benchmark(spark, root: Path, scale: str, partitions: list[int]) -> dict:
    if scale not in ("demo", "medium"):
        raise ValueError("Benchmark supports demo or medium (1M encounters) only")
    root.mkdir(parents=True, exist_ok=True)

    # Step 1: same deterministic source generator used by the portfolio project.
    source, generate_s = elapsed(lambda: generate(spark, root, scale))
    # Step 2: same actual Bronze -> Silver -> Gold pipeline, not a toy stand-in.
    quality, pipeline_s = elapsed(lambda: build(spark, root))
    expected = SCALES[scale].encounters
    assert quality["encounters_in"] == quality["claims_in"] == expected
    assert quality["encounters_valid"] == quality["claims_valid"] == expected
    assert quality["encounters_rejected"] == quality["claims_rejected"] == 0

    # The partition comparison uses identical Silver input and identical
    # calculation in every trial; only upstream partition count changes.
    silver_claims = spark.read.parquet(str(root / "silver" / "claims"))
    input_partitions = silver_claims.rdd.getNumPartitions()
    expected_total = silver_claims.agg(
        F.count("*").alias("n"), F.sum("billed_cents").alias("total")
    ).first()
    expected_count = int(expected_total["n"])
    expected_cents = int(expected_total["total"])
    assert expected_count == expected

    trials = []
    for count in partitions:
        target = root / "benchmark_outputs" / f"partitions_{count}"
        def job():
            grouped = (
                silver_claims.repartition(count, "service_date")
                .groupBy("service_date")
                .agg(F.count("*").alias("claim_count"),
                     F.sum("billed_cents").alias("billed_cents"))
            )
            # Keep Spark's shuffle partition setting stable across trials:
            # a change in repartition() is the independent variable.
            grouped.write.mode("overwrite").parquet(str(target))
            result = spark.read.parquet(str(target))
            check = result.agg(
                F.sum("claim_count").alias("n"),
                F.sum("billed_cents").alias("total")
            ).first()
            return int(check["n"]), int(check["total"]), result.count()
        (rows, cents, groups), duration_s = elapsed(job)
        assert rows == expected_count and cents == expected_cents, (
            "Partition trial failed reconciliation", count, rows, cents
        )
        part_files, part_bytes = count_parquet_files(target)
        trials.append({
            "partitions": count,
            "seconds": duration_s,
            "input_rows": rows,
            "grouped_rows": groups,
            "billed_cents": cents,
            "output_parquet_files": part_files,
            "output_bytes": part_bytes,
            "rows_per_second": round(rows / duration_s, 2) if duration_s else None,
            "reconciled": True,
        })
        print(f"Partitions={count}: {duration_s:.3f}s, "
              f"{rows:,} claims, {part_files} Parquet files")

    bronze_count, bronze_bytes = count_parquet_files(root / "bronze")
    silver_count, silver_bytes = count_parquet_files(root / "silver")
    gold_count, gold_bytes = count_parquet_files(root / "gold")
    manifest = {
        "project": "healthcare-lakehouse-data-platform",
        "mode": scale,
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "spark": spark.version,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            "adaptive_execution": spark.conf.get("spark.sql.adaptive.enabled"),
            "input_claim_partitions": input_partitions,
        },
        "source_counts": source,
        "quality": quality,
        "timings_seconds": {
            "source_generation": generate_s,
            "bronze_to_gold_pipeline": pipeline_s,
            "total_full_pipeline": round(generate_s + pipeline_s, 3),
        },
        "storage": {
            "bronze": {"parquet_files": bronze_count, "bytes": bronze_bytes},
            "silver": {"parquet_files": silver_count, "bytes": silver_bytes},
            "gold": {"parquet_files": gold_count, "bytes": gold_bytes},
        },
        "partition_comparison": {
            "workload": "read Silver claims; repartition by service_date; group by service_date; write Parquet; read back and reconcile",
            "expected_claims": expected_count,
            "expected_billed_cents": expected_cents,
            "trials": trials,
        },
        "caveats": [
            "Single-machine Colab timings are not multi-node cluster results.",
            "Sequential trials can benefit from OS caching or JVM warm-up.",
            "Repartition count is not equivalent to a production Spark tuning study.",
            "Generation and transformation durations exclude pip and Spark startup.",
            "No patient data or PHI; all records are synthetic.",
        ],
    }
    (root / "benchmark_results.json").write_text(json.dumps(manifest, indent=2))
    print("Metrics written to:", root / "benchmark_results.json")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Run a healthcare PySpark benchmark")
    parser.add_argument("--scale", choices=["demo", "medium"], default="demo")
    parser.add_argument("--output-dir", default="outputs/benchmark_demo")
    parser.add_argument("--partitions", default="4,8,16")
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    args = parser.parse_args()
    settings = validate_partition_settings(args.partitions)
    spark = (SparkSession.builder.appName("Healthcare1MBenchmark")
             .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    try:
        report = benchmark(spark, Path(args.output_dir), args.scale, settings)
        print(json.dumps({
            "mode": report["mode"],
            "source_counts": report["source_counts"],
            "timings_seconds": report["timings_seconds"],
            "partition_comparison": report["partition_comparison"]["trials"],
        }, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
