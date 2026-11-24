"""Phase 4 quality gate tests with both clean and intentionally corrupted data."""
from pathlib import Path
import pytest
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F
from src.generate import generate
from src.pipeline import build
from src.data_quality import check_frames, check_outputs
from src.phase4_cli import run_stage


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]")
               .appName("healthcare-quality-tests")
               .config("spark.sql.shuffle.partitions", "4")
               .getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="module")
def prepared(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("healthcare_quality")
    generate(spark, root, "demo")
    build(spark, root)
    frames = {
        name: spark.read.parquet(str(root / "silver" / name))
        for name in ("patients", "providers", "encounters", "claims")
    }
    return root, frames


def test_clean_demo_passes(spark, prepared):
    root, _ = prepared
    report = check_outputs(spark, root, expected={
        "patients": 1000, "providers": 100,
        "encounters": 10000, "claims": 10000,
    })
    assert report["status"] == "PASS"
    assert not report["failed_checks"]
    assert len(report["checks"]) >= 20


def test_detect_bad_fk_and_negative_amount(prepared):
    _, original = prepared
    corrupted = original.copy()
    claims = corrupted["claims"]
    row = claims.limit(1)
    bad = row.withColumn("claim_id", F.lit("CLM-BAD-UNIQUE")) \
             .withColumn("encounter_id", F.lit("ENC-MISSING")) \
             .withColumn("billed_cents", F.lit(-1))
    corrupted["claims"] = claims.unionByName(bad)
    report = check_frames(corrupted)
    assert report["status"] == "FAIL"
    assert "claims.encounter_fk" in report["failed_checks"]
    assert "claims.nonnegative_amount" in report["failed_checks"]


def test_detect_duplicate_claim_key(prepared):
    _, frames = prepared
    corrupted = frames.copy()
    corrupted["claims"] = frames["claims"].unionByName(frames["claims"].limit(1))
    report = check_frames(corrupted)
    assert report["status"] == "FAIL"
    assert "claims.duplicate_key" in report["failed_checks"]


def test_stage_emits_reports(spark, prepared):
    root, _ = prepared
    result = run_stage(spark, "validate", Path(root), scale="demo")
    assert result["status"] == "SUCCESS"
    assert (Path(root) / "monitoring" / "quality_report.json").exists()
    assert (Path(root) / "monitoring" / "validate_latest.json").exists()


def test_bad_expected_counts_fail_closed(spark, prepared):
    root, _ = prepared
    with pytest.raises(RuntimeError):
        run_stage(spark, "validate", Path(root), scale="medium")
    report = (Path(root) / "monitoring" / "quality_report.json").read_text()
    assert '"status": "FAIL"' in report
