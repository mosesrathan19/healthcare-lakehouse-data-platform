"""Phase 4: fail-closed data quality checks for Parquet healthcare layers.

Quality results are JSON-serializable. The validator produces a report even on
quality-gate failures; run status is determined by the CLI, not hidden asserts.
No personal health information or live patient datasets are used.
"""
from __future__ import annotations
from pathlib import Path
from pyspark.sql import functions as F

REQUIRED = {
    "patients": {"patient_id", "date_of_birth", "sex", "state_code"},
    "providers": {"provider_id", "specialty"},
    "encounters": {"encounter_id", "patient_id", "provider_id", "encounter_date", "encounter_type"},
    "claims": {"claim_id", "encounter_id", "billed_cents", "claim_status", "service_date"},
}
KEYS = {"patients": "patient_id", "providers": "provider_id",
        "encounters": "encounter_id", "claims": "claim_id"}


def check_frames(frames: dict, rejected: dict | None = None,
                 gold: dict | None = None, max_reject_fraction: float = 0.0,
                 expected: dict | None = None) -> dict:
    """Return structured checks and FAIL status without silently discarding rows.

    All row counts are measured, not estimated. Foreign-key checks use anti-joins.
    Reconciliation compares Gold totals with fact tables.
    """
    if not 0 <= max_reject_fraction <= 1:
        raise ValueError("max_reject_fraction must be between 0 and 1")
    rejected = rejected or {}
    gold = gold or {}
    expected = expected or {}
    checks = []
    metrics = {}

    def record(name, passed, actual, expected_value, severity="error"):
        checks.append({"check": name, "passed": bool(passed),
                       "actual": actual, "expected": expected_value,
                       "severity": severity})

    for name, required in REQUIRED.items():
        df = frames[name]
        available = set(df.columns)
        missing = sorted(required - available)
        record(f"{name}.schema", not missing, missing, [])
        if missing:
            continue
        n = df.count()
        metrics[name + "_rows"] = n
        record(f"{name}.nonempty", n > 0, n, ">0")
        key = KEYS[name]
        nulls = df.filter(F.col(key).isNull()).count()
        distinct = df.select(key).distinct().count()
        record(f"{name}.null_key", nulls == 0, nulls, 0)
        record(f"{name}.duplicate_key", distinct == n, n - distinct, 0)
        if name in expected:
            record(f"{name}.expected_count", n == expected[name], n, expected[name])

    if any(c["check"].endswith(".schema") and not c["passed"] for c in checks):
        return {"status": "FAIL", "metrics": metrics, "checks": checks}

    encounters, claims = frames["encounters"], frames["claims"]
    patients, providers = frames["patients"], frames["providers"]
    for label, child, column, parent in [
        ("encounters.patient_fk", encounters, "patient_id", patients),
        ("encounters.provider_fk", encounters, "provider_id", providers),
        ("claims.encounter_fk", claims, "encounter_id", encounters),
    ]:
        orphan_count = child.join(parent.select(column).dropDuplicates(), column, "left_anti").count()
        record(label, orphan_count == 0, orphan_count, 0)
    bad_amount = claims.filter(
        F.col("billed_cents").isNull() | (F.col("billed_cents") < 0)).count()
    record("claims.nonnegative_amount", bad_amount == 0, bad_amount, 0)
    bad_status = claims.filter(
        F.col("claim_status").isNull() |
        ~F.col("claim_status").isin("PAID", "DENIED")).count()
    record("claims.allowed_status", bad_status == 0, bad_status, 0)
    invalid_dates = encounters.filter(F.col("encounter_date").isNull()).count()
    record("encounters.valid_date", invalid_dates == 0, invalid_dates, 0)
    claims_dates = claims.filter(F.col("service_date").isNull()).count()
    record("claims.valid_service_date", claims_dates == 0, claims_dates, 0)

    for name in ("encounters", "claims"):
        clean = metrics.get(name + "_rows", 0)
        quarantine = rejected.get(name)
        bad = quarantine.count() if quarantine is not None else 0
        metrics[name + "_rejected_rows"] = bad
        fraction = bad / (clean + bad) if clean + bad else 1.0
        record(f"{name}.reject_rate", fraction <= max_reject_fraction,
               round(fraction, 8), f"<= {max_reject_fraction}")
        if name in expected:
            record(f"{name}.source_reconciliation",
                   clean + bad == expected[name], clean + bad, expected[name])

    if "claims_by_specialty" in gold and "claims_by_day" in gold:
        n_claims = metrics.get("claims_rows")
        gold_specialty = gold["claims_by_specialty"].agg(
            F.sum("claim_count").alias("n")).first()["n"]
        gold_day = gold["claims_by_day"].agg(
            F.sum("claims").alias("n")).first()["n"]
        record("gold.specialty_count_reconciles", gold_specialty == n_claims,
               gold_specialty, n_claims)
        record("gold.daily_count_reconciles", gold_day == n_claims,
               gold_day, n_claims)
        expected_paid = claims.agg(F.sum(
            F.when(F.col("claim_status") == "PAID",
                   F.col("billed_cents")).otherwise(F.lit(0))).alias("v")).first()["v"]
        specialty_paid = gold["claims_by_specialty"].agg(
            F.sum("paid_cents").alias("v")).first()["v"]
        record("gold.paid_amount_reconciles",
               specialty_paid == expected_paid, specialty_paid, expected_paid)

    failed = [c["check"] for c in checks
              if c["severity"] == "error" and not c["passed"]]
    return {"status": "FAIL" if failed else "PASS", "metrics": metrics,
            "checks": checks, "failed_checks": failed}


def check_outputs(spark, root: str | Path, max_reject_fraction: float = 0,
                  expected: dict | None = None) -> dict:
    root = Path(root)
    frames = {name: spark.read.parquet(str(root / "silver" / name))
              for name in REQUIRED}
    quarantine = {
        name: spark.read.parquet(str(root / "silver" / f"rejected_{name}"))
        for name in ("encounters", "claims")
    }
    gold = {name: spark.read.parquet(str(root / "gold" / name))
            for name in ("claims_by_specialty", "claims_by_day")}
    return check_frames(frames, quarantine, gold,
                        max_reject_fraction=max_reject_fraction, expected=expected)
