"""CLI: python -m src.cli --scale demo --output-dir outputs/demo."""
import argparse
import json
from pyspark.sql import SparkSession
from src.generate import generate
from src.pipeline import build

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", choices=["demo", "medium", "large"], default="demo")
    p.add_argument("--output-dir", default="outputs/demo")
    p.add_argument("--shuffle-partitions", type=int, default=16)
    args = p.parse_args()
    spark = (SparkSession.builder.appName("SyntheticHealthcareLakehouse")
             .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
             .getOrCreate())
    try:
        source = generate(spark, args.output_dir, args.scale)
        quality = build(spark, args.output_dir)
        print(json.dumps({"source": source, "quality": quality}, indent=2))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
