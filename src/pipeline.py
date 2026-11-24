"""Bronze → Silver → Gold batch pipeline with quality reconciliation.

This starter performs full-refresh Parquet writes. CDC and transactional MERGE
are roadmap items, not implemented here.
"""
from pathlib import Path
import json
from pyspark.sql import functions as F


def require_unique(frame, key: str):
    if frame.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Duplicate key: {key}")
    if frame.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"Null primary key: {key}")


def build(spark, root: str | Path) -> dict:
    root = Path(root)
    bronze = root / "bronze"
    patients = spark.read.parquet(str(bronze / "patients"))
    providers = spark.read.parquet(str(bronze / "providers"))
    encounters = spark.read.parquet(str(bronze / "encounters"))
    claims = spark.read.parquet(str(bronze / "claims"))

    for frame, key in [
        (patients, "patient_id"), (providers, "provider_id"),
        (encounters, "encounter_id"), (claims, "claim_id"),
    ]:
        require_unique(frame, key)

    valid_encounters = (
        encounters.join(patients.select("patient_id"), "patient_id", "left_semi")
        .join(providers.select("provider_id"), "provider_id", "left_semi")
    )
    rejected_encounters = encounters.join(
        valid_encounters.select("encounter_id"), "encounter_id", "left_anti"
    )
    valid_claims = claims.join(
        valid_encounters.select("encounter_id"), "encounter_id", "left_semi"
    )
    rejected_claims = claims.join(
        valid_claims.select("claim_id"), "claim_id", "left_anti"
    )

    quality = {
        "patients": patients.count(),
        "providers": providers.count(),
        "encounters_in": encounters.count(),
        "encounters_valid": valid_encounters.count(),
        "encounters_rejected": rejected_encounters.count(),
        "claims_in": claims.count(),
        "claims_valid": valid_claims.count(),
        "claims_rejected": rejected_claims.count(),
    }
    assert quality["encounters_in"] == quality["encounters_valid"] + quality["encounters_rejected"]
    assert quality["claims_in"] == quality["claims_valid"] + quality["claims_rejected"]

    silver = root / "silver"
    for name, frame in {
        "patients": patients, "providers": providers,
        "encounters": valid_encounters, "claims": valid_claims,
        "rejected_encounters": rejected_encounters, "rejected_claims": rejected_claims,
    }.items():
        frame.write.mode("overwrite").parquet(str(silver / name))

    fact = (
        valid_claims.join(valid_encounters, "encounter_id", "inner")
        .join(providers, "provider_id", "inner")
        .withColumn(
            "paid_cents",
            F.when(F.col("claim_status") == "PAID", F.col("billed_cents"))
             .otherwise(F.lit(0)),
        )
    )
    by_specialty = fact.groupBy("specialty").agg(
        F.count("claim_id").alias("claim_count"),
        F.sum("billed_cents").alias("billed_cents"),
        F.sum("paid_cents").alias("paid_cents"),
        F.sum((F.col("claim_status") == "DENIED").cast("int")).alias("denied_count"),
    )
    by_day = fact.groupBy("encounter_date").agg(
        F.count("claim_id").alias("claims"),
        F.sum("paid_cents").alias("paid_cents"),
    )
    gold = root / "gold"
    by_specialty.write.mode("overwrite").parquet(str(gold / "claims_by_specialty"))
    by_day.write.mode("overwrite").parquet(str(gold / "claims_by_day"))
    assert by_specialty.agg(F.sum("claim_count")).first()[0] == quality["claims_valid"]

    (root / "quality_report.json").write_text(json.dumps(quality, indent=2))
    return quality
