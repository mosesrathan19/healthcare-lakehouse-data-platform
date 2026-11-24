"""Phase 2: deterministic synthetic claims CDC with transactional Delta Lake MERGE.

A self-contained educational example: use only fabricated patient/claim data.
Requires pyspark 3.5.x + delta-spark 3.3.x with a Delta-enabled SparkSession.
"""
from pathlib import Path
import json
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BATCH = "demo-2026-11-24"
BASE_VERSION = "2025-01-01 00:00:00"
NEW_VERSION = "2026-11-24 12:00:00"
OLD_VERSION = "2026-11-24 11:00:00"

def synthetic_events(spark, bronze_claims):
    """100 inserts, 20 updates, 5 deletes, 5 stale updates => 130 events."""
    cols = ["claim_id", "encounter_id", "billed_cents", "claim_status", "service_date"]
    # All events are deterministically constructed from immutable Bronze records,
    # not the current Delta snapshot. This makes batch replay meaningful.
    ids = F.substring("claim_id", 4, 12).cast("long")
    inserted = (bronze_claims.filter((ids >= 1) & (ids <= 100))
                .withColumn("claim_id", F.format_string("CLM%012d", ids + F.lit(10000)))
                .withColumn("billed_cents", F.col("billed_cents") + 2000)
                .select(*cols).withColumn("op", F.lit("I"))
                .withColumn("event_version", F.to_timestamp(F.lit(NEW_VERSION))))
    updated = (bronze_claims.filter((ids >= 1) & (ids <= 20))
               .withColumn("billed_cents", F.col("billed_cents") + 500)
               .select(*cols).withColumn("op", F.lit("U"))
               .withColumn("event_version", F.to_timestamp(F.lit(NEW_VERSION))))
    deleted = (bronze_claims.filter((ids >= 21) & (ids <= 25))
               .select(*cols).withColumn("op", F.lit("D"))
               .withColumn("event_version", F.to_timestamp(F.lit(NEW_VERSION))))
    stale = (bronze_claims.filter((ids >= 1) & (ids <= 5))
             .withColumn("billed_cents", F.col("billed_cents") + 9999)
             .select(*cols).withColumn("op", F.lit("U"))
             .withColumn("event_version", F.to_timestamp(F.lit(OLD_VERSION))))
    return inserted.unionByName(updated).unionByName(deleted).unionByName(stale)


def latest_events(events):
    bad = events.filter(
        F.col("claim_id").isNull() | F.col("event_version").isNull()
        | ~F.col("op").isin("I", "U", "D")
    )
    if bad.limit(1).count():
        raise ValueError("Invalid CDC record")
    ambiguous = (events.groupBy("claim_id", "event_version")
                 .count().filter(F.col("count") > 1))
    if ambiguous.limit(1).count():
        raise ValueError("Conflicting CDC events have the same key and version")
    w = Window.partitionBy("claim_id").orderBy(F.col("event_version").desc())
    return (events.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))


def merge_claims(spark, delta_path, events):
    """Atomic Delta MERGE. Repeat of same event version makes no changes.

    No matched event overwrites a newer version. Deletes remove the row; for a
    production CDC feed, keep tombstone versions to reject late resurrections.
    """
    effective = latest_events(events)
    target = DeltaTable.forPath(spark, str(delta_path))
    (target.alias("t").merge(effective.alias("s"), "t.claim_id = s.claim_id")
     .whenMatchedDelete(condition="s.op = 'D' AND s.event_version > t.event_version")
     .whenMatchedUpdate(
         condition="s.op IN ('I', 'U') AND s.event_version > t.event_version",
         set={
             "encounter_id": "s.encounter_id",
             "billed_cents": "s.billed_cents",
             "claim_status": "s.claim_status",
             "service_date": "s.service_date",
             "event_version": "s.event_version",
         })
     .whenNotMatchedInsert(
         condition="s.op = 'I'",
         values={
             "claim_id": "s.claim_id",
             "encounter_id": "s.encounter_id",
             "billed_cents": "s.billed_cents",
             "claim_status": "s.claim_status",
             "service_date": "s.service_date",
             "event_version": "s.event_version",
         })
     .execute())
    return spark.read.format("delta").load(str(delta_path))


def run(spark, output_dir, batch=BATCH):
    """Initialize from starter Silver, apply deterministic CDC and publish Gold.

    Safe for sequential replay of this demo batch, not concurrent writers.
    """
    output_dir = Path(output_dir)
    bronze = output_dir / "bronze" / "claims"
    silver = output_dir / "silver" / "claims"
    if not bronze.exists() or not silver.exists():
        raise FileNotFoundError("Run python -m src.cli --scale demo first")
    delta_path = output_dir / "delta" / "claims"
    if not DeltaTable.isDeltaTable(spark, str(delta_path)):
        baseline = (spark.read.parquet(str(silver))
                    .withColumn("event_version", F.to_timestamp(F.lit(BASE_VERSION))))
        baseline.write.format("delta").mode("errorifexists").save(str(delta_path))
    original = spark.read.parquet(str(bronze))
    if original.count() != 10000:
        raise ValueError("This demo CDC batch is defined for the 10K-claim dataset only")
    events = synthetic_events(spark, original)
    event_count = events.count()
    effective = latest_events(events)
    effective_count = effective.count()
    if (event_count, effective_count) != (130, 125):
        raise AssertionError((event_count, effective_count))
    before = spark.read.format("delta").load(str(delta_path)).count()
    after_df = merge_claims(spark, delta_path, events)
    after = after_df.count()
    if after != 10095:
        raise AssertionError(f"Expected 10095 current claims, got {after}")
    if after_df.select("claim_id").distinct().count() != after:
        raise AssertionError("Duplicate primary keys")
    if after_df.filter(F.col("claim_id") == "CLM000000000021").count():
        raise AssertionError("Delete did not apply")
    if after_df.filter(
        (F.col("claim_id") == "CLM000000000001") & (F.col("billed_cents") != 10500)
    ).count():
        raise AssertionError("Latest update not applied")
    # Validate claim foreign keys against unchanged Silver encounters.
    encounter_keys = spark.read.parquet(str(output_dir / "silver" / "encounters")).select("encounter_id")
    if after_df.join(encounter_keys, "encounter_id", "left_anti").limit(1).count():
        raise AssertionError("Orphaned claims found")
    # Reporting tables represent the new committed Delta state, not raw CDC.
    fact = (after_df.join(encounter_keys, "encounter_id", "inner")
            .withColumn("paid_cents", F.when(F.col("claim_status") == "PAID",
                                              F.col("billed_cents")).otherwise(F.lit(0))))
    gold = fact.groupBy("claim_status").agg(
        F.count("*").alias("claims"),
        F.sum("billed_cents").alias("billed_cents"),
        F.sum("paid_cents").alias("paid_cents"),
    )
    gold.write.mode("overwrite").parquet(str(output_dir / "delta_gold" / "claims_by_status"))
    assert gold.agg(F.sum("claims")).first()[0] == after
    report = {"batch": batch, "baseline_claims": 10000,
              "events": event_count, "effective_events": effective_count,
              "current_claims": after, "claims_before_this_run": before,
              "expected_inserted": 100, "expected_updated": 20,
              "expected_deleted": 5}
    (output_dir / "delta_cdc_report.json").write_text(json.dumps(report, indent=2))
    return report
