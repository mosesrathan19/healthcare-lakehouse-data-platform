"""Phase 5: generate synthetic Bronze/Silver/Gold as UC managed Delta tables.

Default is a zero-write execution plan. Only user opt-in writes cloud tables.
A Free Edition workspace's Unity Catalog default managed storage is enough:
no external S3/IAM role or paid cloud resources required for this path.
"""
from __future__ import annotations
import re
import time
from pyspark.sql import functions as F
from src.config import SCALES

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NAMES = ("patients", "providers", "encounters", "claims")


def identifiers(catalog: str, schema: str) -> tuple[str, str]:
    if not IDENTIFIER.fullmatch(catalog) or not IDENTIFIER.fullmatch(schema):
        raise ValueError("Catalog and schema must be plain SQL identifiers")
    if catalog.lower() == "hive_metastore":
        raise ValueError("Use a Unity Catalog catalog, not hive_metastore")
    return catalog, schema


def plan(catalog: str, schema: str, scale="demo") -> dict:
    identifiers(catalog, schema)
    if scale not in ("demo", "medium"):
        raise ValueError("Only demo/medium are supported; large is blocked")
    counts = SCALES[scale]
    return {"catalog": catalog, "schema": schema, "scale": scale,
            "patients": counts.patients, "providers": counts.providers,
            "encounters": counts.encounters, "claims": counts.encounters,
            "table_format": "delta", "table_type": "Unity Catalog managed",
            "tables": [f"{catalog}.{schema}.{layer}_{name}"
                       for layer in ("bronze", "silver")
                       for name in NAMES] +
                      [f"{catalog}.{schema}.gold_claims_by_status"],
            "warning": "Opt-in execute overwrites these named tables. Cloud compute/storage may be billable."}


def frames(spark, scale="demo"):
    """Create the same deterministic synthetic rows as src/generate.py."""
    s = SCALES[scale]
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
    return dict(patients=patients, providers=providers,
                encounters=encounters, claims=claims)


def run(spark, catalog: str, schema: str, scale="demo", *, execute=False):
    spec = plan(catalog, schema, scale)
    if not execute:
        return {"status": "DRY_RUN", **spec}
    # Pre-flight permissions must be verified by the user, and managed
    # schema creation requires appropriate UC privileges.
    start = time.perf_counter()
    ns = f"`{catalog}`.`{schema}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")
    data = frames(spark, scale)
    def save(name, df):
        fq = f"{ns}.`{name}`"
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(fq))

    for name in NAMES:
        save("bronze_" + name, data[name])

    # Silver references validate existing patient/provider/encounter keys.
    patients = spark.table(f"{ns}.`bronze_patients`")
    providers = spark.table(f"{ns}.`bronze_providers`")
    encounters = spark.table(f"{ns}.`bronze_encounters`")
    claims = spark.table(f"{ns}.`bronze_claims`")
    clean_encounters = (encounters
         .join(patients.select("patient_id"), "patient_id", "left_semi")
         .join(providers.select("provider_id"), "provider_id", "left_semi"))
    clean_claims = claims.join(
        clean_encounters.select("encounter_id"), "encounter_id", "left_semi")
    for name, frame in (("patients", patients), ("providers", providers),
                        ("encounters", clean_encounters), ("claims", clean_claims)):
        save("silver_" + name, frame)

    silver_encounters = spark.table(f"{ns}.`silver_encounters`")
    silver_claims = spark.table(f"{ns}.`silver_claims`")
    if silver_encounters.count() != spec["encounters"] or silver_claims.count() != spec["claims"]:
        raise AssertionError("Silver counts mismatch; do not publish Gold")
    if silver_claims.filter(F.col("billed_cents") < 0).limit(1).count():
        raise AssertionError("Negative billed cents")

    gold = (silver_claims.groupBy("claim_status").agg(
        F.count("*").alias("claim_count"),
        F.sum("billed_cents").alias("billed_cents")))
    save("gold_claims_by_status", gold)
    gold_result = spark.table(f"{ns}.`gold_claims_by_status`")
    gold_n = gold_result.agg(F.sum("claim_count")).first()[0]
    gold_cents = gold_result.agg(F.sum("billed_cents")).first()[0]
    silver_cents = silver_claims.agg(F.sum("billed_cents")).first()[0]
    if gold_n != spec["claims"] or gold_cents != silver_cents:
        raise AssertionError("Gold/Silver reconciliation failed")
    return {"status": "PASS", "elapsed_seconds": round(time.perf_counter()-start, 3),
            "claims": int(gold_n), "billed_cents": int(gold_cents), **spec}
