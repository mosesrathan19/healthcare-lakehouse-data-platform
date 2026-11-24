"""Deterministic, fully synthetic healthcare source generator.

Spark range() generates data without collecting source rows on the driver.
Do not use real patient records for this demonstration.
"""
from pathlib import Path
from pyspark.sql import functions as F
from src.config import SCALES


def generate(spark, output_root: str | Path, scale: str = "demo") -> dict:
    if scale not in SCALES:
        raise ValueError(f"Unknown scale: {scale}. Choose {sorted(SCALES)}")
    s = SCALES[scale]
    root = Path(output_root) / "bronze"

    patients = spark.range(s.patients).select(
        F.format_string("PAT%09d", F.col("id") + 1).alias("patient_id"),
        F.expr("date_add(date'1940-01-01', cast(id * 97 % 25000 as int))").alias("date_of_birth"),
        F.when(F.col("id") % 3 == 0, "F").when(F.col("id") % 3 == 1, "M").otherwise("X").alias("sex"),
        F.format_string("%02d", F.col("id") % 50 + 1).alias("state_code"),
    )
    providers = spark.range(s.providers).select(
        F.format_string("PRV%07d", F.col("id") + 1).alias("provider_id"),
        F.when(F.col("id") % 3 == 0, "Primary Care")
         .when(F.col("id") % 3 == 1, "Cardiology").otherwise("Ophthalmology").alias("specialty"),
    )
    encounters = spark.range(s.encounters).select(
        F.format_string("ENC%012d", F.col("id") + 1).alias("encounter_id"),
        F.format_string("PAT%09d", F.col("id") % s.patients + 1).alias("patient_id"),
        F.format_string("PRV%07d", (F.col("id") * 7) % s.providers + 1).alias("provider_id"),
        F.expr("date_add(date'2025-01-01', cast(id % 365 as int))").alias("encounter_date"),
        F.when(F.col("id") % 10 == 0, "INPATIENT").otherwise("OUTPATIENT").alias("encounter_type"),
    )
    claims = spark.range(s.encounters).select(
        F.format_string("CLM%012d", F.col("id") + 1).alias("claim_id"),
        F.format_string("ENC%012d", F.col("id") + 1).alias("encounter_id"),
        (F.lit(10000) + (F.col("id") % 4000) * 50).cast("long").alias("billed_cents"),
        F.when(F.col("id") % 11 == 0, "DENIED").otherwise("PAID").alias("claim_status"),
        F.expr("date_add(date'2025-01-01', cast(id % 365 as int))").alias("service_date"),
    )
    for name, frame in {
        "patients": patients,
        "providers": providers,
        "encounters": encounters,
        "claims": claims,
    }.items():
        frame.write.mode("overwrite").parquet(str(root / name))
    return {
        "patients": s.patients,
        "providers": s.providers,
        "encounters": s.encounters,
        "claims": s.encounters,
    }
