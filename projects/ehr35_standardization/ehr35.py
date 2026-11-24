"""Synthetic 35-source EHR standardization: configurable mappings and data generator.

This is a synthetic reconstruction, NOT the original university project and NOT
real Epic/NextGen/EyeMD integration. Avoid using any PHI.
"""
from __future__ import annotations
from pathlib import Path
import json
from pyspark.sql import functions as F

SOURCE_COUNT = 35
FAMILIES = ("clinical_a", "clinical_b", "clinical_c", "clinical_d", "clinical_e")
CANONICAL_FIELDS = ("patient_id", "encounter_id", "provider_id",
                    "encounter_date", "diagnosis_code", "billed_cents")
VARIANTS = {
    "clinical_a": {"patient_id": "patient_id", "encounter_id": "visit_id",
                   "provider_id": "doctor_id", "encounter_date": "visit_date",
                   "diagnosis_code": "dx_code", "billed_cents": "charge_cents"},
    "clinical_b": {"patient_id": "pat_key", "encounter_id": "enc_key",
                   "provider_id": "prov_key", "encounter_date": "enc_dt",
                   "diagnosis_code": "icd", "billed_cents": "amount_cents"},
    "clinical_c": {"patient_id": "person_ref", "encounter_id": "episode_ref",
                   "provider_id": "clinician_ref", "encounter_date": "service_dt",
                   "diagnosis_code": "diagnosis", "billed_cents": "amount"},
    "clinical_d": {"patient_id": "member_id", "encounter_id": "claim_visit",
                   "provider_id": "attending_id", "encounter_date": "date_seen",
                   "diagnosis_code": "diagnosis_code", "billed_cents": "bill"},
    "clinical_e": {"patient_id": "subject_id", "encounter_id": "event_id",
                   "provider_id": "staff_id", "encounter_date": "event_date",
                   "diagnosis_code": "code", "billed_cents": "gross_cents"},
}


def registry() -> list[dict]:
    """35 distinct source identifiers, five schema families, seven sites each."""
    return [{"source_id": f"ehr_{idx:02d}", "family": FAMILIES[(idx - 1) % 5],
             "site": (idx - 1) // 5 + 1} for idx in range(1, SOURCE_COUNT + 1)]


def create_sources(spark, root: str | Path, rows_per_source: int = 100):
    """Write independent fabricated source Parquet datasets and registry JSON."""
    if not 1 <= rows_per_source <= 100000:
        raise ValueError("rows_per_source must be 1..100000")
    root = Path(root)
    base = root / "source"
    for item in registry():
        idx = int(item["source_id"].split("_")[1])
        n = rows_per_source
        source = spark.range(n).select(
            F.format_string("P%03d-%06d", F.lit(idx), F.col("id") + 1).alias("patient_id"),
            F.format_string("E%03d-%06d", F.lit(idx), F.col("id") + 1).alias("encounter_id"),
            F.format_string("D%04d", F.col("id") % 25 + 1).alias("provider_id"),
            F.expr("date_add(date'2025-01-01', cast(id % 365 as int))").alias("encounter_date"),
            F.when(F.col("id") % 4 == 0, "E11.9")
             .when(F.col("id") % 4 == 1, "I10")
             .when(F.col("id") % 4 == 2, "J45.909")
             .otherwise("Z00.00").alias("diagnosis_code"),
            (F.lit(5000) + (F.col("id") % 200) * 125).cast("long").alias("billed_cents"),
        )
        mapping = VARIANTS[item["family"]]
        for canonical, source_col in mapping.items():
            source = source.withColumnRenamed(canonical, source_col)
        source.write.mode("overwrite").parquet(str(base / item["source_id"]))
    (root / "source_registry.json").write_text(json.dumps(registry(), indent=2))
    return {"source_count": SOURCE_COUNT, "rows_per_source": rows_per_source,
            "expected_rows": SOURCE_COUNT * rows_per_source}


def standardize_source(frame, source_id: str, family: str):
    if family not in VARIANTS:
        raise ValueError("Unknown schema family: " + family)
    mapping = VARIANTS[family]
    missing = set(mapping.values()) - set(frame.columns)
    if missing:
        raise ValueError(f"{source_id}: missing source columns {sorted(missing)}")
    return frame.select(
        F.lit(source_id).alias("source_system"),
        F.col(mapping["patient_id"]).cast("string").alias("patient_id"),
        F.col(mapping["encounter_id"]).cast("string").alias("encounter_id"),
        F.col(mapping["provider_id"]).cast("string").alias("provider_id"),
        F.to_date(F.col(mapping["encounter_date"])).alias("encounter_date"),
        F.col(mapping["diagnosis_code"]).cast("string").alias("diagnosis_code"),
        F.col(mapping["billed_cents"]).cast("long").alias("billed_cents"),
    )
