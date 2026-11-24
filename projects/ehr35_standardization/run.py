"""Run a reproducible 35-source synthetic EHR consolidation pipeline.

Usage: python -m projects.ehr35_standardization.run --output-dir outputs/ehr35
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import time
from functools import reduce
from pyspark.sql import SparkSession, functions as F
from .ehr35 import registry, create_sources, standardize_source, SOURCE_COUNT


def consolidate(spark, root: Path, rows_per_source: int = 100) -> dict:
    start = time.perf_counter()
    generated = create_sources(spark, root, rows_per_source)
    frames = []
    per_source = {}
    for item in registry():
        name = item["source_id"]
        raw = spark.read.parquet(str(root / "source" / name))
        standardized = standardize_source(raw, name, item["family"])
        frames.append(standardized)
        per_source[name] = rows_per_source
    unified = reduce(lambda left, right: left.unionByName(right), frames)

    invalid = (F.col("patient_id").isNull() | F.col("encounter_id").isNull()
               | F.col("provider_id").isNull() | F.col("encounter_date").isNull()
               | F.col("diagnosis_code").isNull()
               | F.col("billed_cents").isNull() | (F.col("billed_cents") < 0))
    rejected = unified.filter(invalid)
    accepted = unified.filter(~invalid)
    bronze_count = unified.count()
    rejected_count = rejected.count()
    clean_count = accepted.count()
    duplicate_count = (accepted.groupBy("source_system", "encounter_id")
                       .count().filter(F.col("count") > 1).count())
    observed_sources = accepted.select("source_system").distinct().count()
    assert bronze_count == generated["expected_rows"]
    assert bronze_count == clean_count + rejected_count
    assert duplicate_count == 0
    assert observed_sources == SOURCE_COUNT
    assert rejected_count == 0

    # Persist cleaned canonical encounters and quarantine separately.
    accepted.write.mode("overwrite").parquet(str(root / "silver" / "encounters"))
    rejected.write.mode("overwrite").parquet(str(root / "silver" / "quarantined_encounters"))

    # Illustrative analytics ONLY, not clinical quality measure certification.
    by_source = accepted.groupBy("source_system").agg(
        F.count("*").alias("encounters"),
        F.sum("billed_cents").alias("billed_cents"))
    by_diagnosis = accepted.groupBy("diagnosis_code").agg(
        F.count("*").alias("encounters"),
        F.sum("billed_cents").alias("billed_cents"))
    by_source.write.mode("overwrite").parquet(str(root / "gold" / "encounters_by_source"))
    by_diagnosis.write.mode("overwrite").parquet(str(root / "gold" / "encounters_by_diagnosis"))
    gold_total = by_source.agg(F.sum("encounters")).first()[0]
    amount_silver = accepted.agg(F.sum("billed_cents")).first()[0]
    amount_gold = by_source.agg(F.sum("billed_cents")).first()[0]
    assert gold_total == clean_count
    assert amount_silver == amount_gold

    report = {
        "status": "PASS", "data_classification": "synthetic",
        "source_systems": SOURCE_COUNT, "schema_families": 5,
        "rows_per_source": rows_per_source,
        "input_encounters": bronze_count, "accepted_encounters": clean_count,
        "quarantined_encounters": rejected_count,
        "duplicate_source_encounter_keys": duplicate_count,
        "observed_sources": observed_sources,
        "billed_cents": int(amount_silver),
        "elapsed_seconds": round(time.perf_counter() - start, 3),
        "scope": "Demonstration only; not live EHR integration or eCQM/MIPS certification",
    }
    (root / "ehr35_quality_report.json").write_text(json.dumps(report, indent=2))
    return report


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/ehr35")
    p.add_argument("--rows-per-source", type=int, default=100)
    args = p.parse_args()
    spark = (SparkSession.builder.appName("Synthetic35EHRStandardization")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    try:
        print(json.dumps(consolidate(spark, Path(args.output_dir),
                                     args.rows_per_source), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
