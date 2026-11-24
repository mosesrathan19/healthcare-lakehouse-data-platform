"""PySpark integration tests; run in an environment with Java and Spark."""
import pytest
pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from src.generate import generate
from src.pipeline import build


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]").appName("healthcare-tests")
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()
    )
    yield session
    session.stop()


def test_demo_end_to_end(spark, tmp_path):
    expected = generate(spark, tmp_path, "demo")
    assert expected == {
        "patients": 1000, "providers": 100,
        "encounters": 10000, "claims": 10000,
    }
    quality = build(spark, tmp_path)
    assert quality["encounters_valid"] == 10000
    assert quality["claims_valid"] == 10000
    assert quality["encounters_rejected"] == 0
    assert quality["claims_rejected"] == 0
    gold = spark.read.parquet(str(tmp_path / "gold" / "claims_by_specialty"))
    assert gold.count() == 3
