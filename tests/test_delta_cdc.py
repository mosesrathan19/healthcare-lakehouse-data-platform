"""Healthcare claims CDC tests.

Run lightweight event tests with:
    pip install 'pyspark>=3.5.3,<3.6' 'delta-spark>=3.3,<3.4'
    pytest -q tests/test_delta_cdc.py

Run full Delta integration test with RUN_DELTA_INTEGRATION=1.
Requires a Delta-enabled local Spark runtime and Maven access.
"""
import os
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
from pyspark.sql import SparkSession, functions as F
from src.generate import generate
from src.pipeline import build
from src.delta_cdc import synthetic_events, latest_events, run


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master("local[2]").appName("healthcare-cdc-tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "4"))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def test_event_dedup(spark):
    source = spark.range(10000).select(
        F.format_string("CLM%012d", F.col("id") + 1).alias("claim_id"),
        F.format_string("ENC%012d", F.col("id") + 1).alias("encounter_id"),
        (F.lit(10000) + F.col("id") * 50).cast("long").alias("billed_cents"),
        F.lit("PAID").alias("claim_status"),
        F.to_date(F.lit("2025-01-01")).alias("service_date"),
    )
    events = synthetic_events(spark, source)
    assert events.count() == 130
    assert latest_events(events).count() == 125
    assert latest_events(events).filter(
        F.col("event_version") == F.to_timestamp(F.lit("2026-11-24 11:00:00"))
    ).count() == 0


@pytest.mark.skipif(os.environ.get("RUN_DELTA_INTEGRATION") != "1",
                    reason="Set RUN_DELTA_INTEGRATION=1 to test Delta MERGE end-to-end")
def test_delta_merge_replay(spark, tmp_path):
    generate(spark, tmp_path, "demo")
    build(spark, tmp_path)
    first = run(spark, tmp_path)
    second = run(spark, tmp_path)
    assert first["current_claims"] == second["current_claims"] == 10095
    assert second["claims_before_this_run"] == 10095
