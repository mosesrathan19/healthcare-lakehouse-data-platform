"""Smoke and configuration checks for the healthcare PySpark benchmark."""
import pytest
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from src.benchmark import benchmark, validate_partition_settings

def test_partitions():
    assert validate_partition_settings("2,4,8") == [2, 4, 8]
    for bad in ("", "0", "2,2", "257", "oops"):
        with pytest.raises(ValueError):
            validate_partition_settings(bad)

def test_demo_smoke(tmp_path):
    spark = (SparkSession.builder.master("local[2]")
             .appName("benchmark-smoke")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())
    try:
        report = benchmark(spark, tmp_path / "benchmark", "demo", [2, 4])
        assert report["quality"]["claims_valid"] == 10000
        assert report["quality"]["encounters_valid"] == 10000
        assert all(t["input_rows"] == 10000 and t["reconciled"]
                   for t in report["partition_comparison"]["trials"])
        assert (tmp_path / "benchmark" / "benchmark_results.json").exists()
    finally:
        spark.stop()
