"""Phase 4 command-line stages for Airflow or Google Colab.

Each stage emits a JSON run event with UTC timestamp, runtime and outcome.
The validation stage always saves a report before failing a quality gate.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import time
import traceback
from pyspark.sql import SparkSession
from src.config import SCALES
from src.generate import generate
from src.pipeline import build
from src.data_quality import check_outputs


def stage(spark, name: str, root: Path, scale: str, max_reject_fraction: float):
    if name == "generate":
        return generate(spark, root, scale)
    if name == "build":
        return build(spark, root)
    if name == "validate":
        settings = SCALES[scale]
        expected = {"patients": settings.patients,
                    "providers": settings.providers,
                    "encounters": settings.encounters,
                    "claims": settings.encounters}
        return check_outputs(spark, root, max_reject_fraction, expected)
    raise ValueError(f"Unknown stage {name}")


def run_stage(spark, name: str, root: Path, scale="demo",
              max_reject_fraction: float = 0) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    audit_dir = root / "monitoring"
    audit_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    start = time.perf_counter()
    event = {
        "stage": name, "scale": scale,
        "start_utc": started.isoformat(), "spark": spark.version,
        "python": platform.python_version(),
        "status": "RUNNING",
    }
    try:
        result = stage(spark, name, root, scale, max_reject_fraction)
        event["result"] = result
        event["status"] = "FAILED" if result.get("status") == "FAIL" else "SUCCESS"
    except Exception as exc:
        event["status"] = "FAILED"
        event["error"] = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        event["elapsed_seconds"] = round(time.perf_counter() - start, 3)
        event["end_utc"] = datetime.now(timezone.utc).isoformat()
        # A per-stage latest report makes retries observable. A production
        # monitor should write append-only events to durable shared storage.
        (audit_dir / f"{name}_latest.json").write_text(json.dumps(event, indent=2, default=str))
        if name == "validate" and isinstance(event.get("result"), dict):
            (audit_dir / "quality_report.json").write_text(
                json.dumps(event["result"], indent=2, default=str))
        print(json.dumps(event, indent=2, default=str))
    if event["status"] != "SUCCESS":
        raise RuntimeError(f"Stage {name} failed; inspect {audit_dir / (name + '_latest.json')}")
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", required=True, choices=["generate", "build", "validate"])
    parser.add_argument("--scale", default="demo", choices=sorted(SCALES))
    parser.add_argument("--output-dir", default="outputs/phase4_demo")
    parser.add_argument("--max-reject-fraction", default=0.0, type=float)
    args = parser.parse_args()
    if not 0 <= args.max_reject_fraction <= 1:
        parser.error("--max-reject-fraction must be between 0 and 1")
    spark = (SparkSession.builder.appName("HealthcareQualityOrchestration")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    try:
        run_stage(spark, args.stage, Path(args.output_dir),
                  args.scale, args.max_reject_fraction)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
