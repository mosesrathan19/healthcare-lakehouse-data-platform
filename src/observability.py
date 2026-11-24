"""Local structured operational monitoring for synthetic batch-pipeline runs.

Emits append-only JSONL (locally) and evaluates configurable thresholds.
No network calls, alerts, dashboards, SaaS credentials or PHI.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

ALLOWED_FIELDS = ("stage", "status", "elapsed_seconds", "claims_valid",
                  "claims_rejected", "encounters_valid", "encounters_rejected",
                  "input_rows", "run_id", "source")


def sanitize(event: dict) -> dict:
    """Whitelist metrics to avoid accidentally logging healthcare fields."""
    result = {key: event[key] for key in ALLOWED_FIELDS if key in event}
    if result.get("status") not in ("SUCCESS", "FAILED", "PASS", "FAIL"):
        raise ValueError("Unknown status")
    if not result.get("stage"):
        raise ValueError("Missing stage")
    for key in ("elapsed_seconds", "claims_valid", "claims_rejected",
                "encounters_valid", "encounters_rejected", "input_rows"):
        if key in result and (not isinstance(result[key], (int, float))
                              or isinstance(result[key], bool)
                              or result[key] < 0):
            raise ValueError("Invalid numeric metric: " + key)
    return result


def evaluate(event: dict, *, max_seconds: float = 600,
             max_rejected_fraction: float = 0.0) -> dict:
    """Fail closed for errors, slow runs, and too many rejects."""
    if max_seconds <= 0 or not 0 <= max_rejected_fraction <= 1:
        raise ValueError("Invalid threshold")
    event = sanitize(event)
    failed = []
    if event["status"] in ("FAILED", "FAIL"):
        failed.append("stage_failed")
    if event.get("elapsed_seconds", 0) > max_seconds:
        failed.append("duration_threshold")
    for name in ("claims", "encounters"):
        valid = event.get(name + "_valid")
        rejected = event.get(name + "_rejected")
        if valid is not None and rejected is not None:
            total = valid + rejected
            ratio = rejected / total if total else 1.0
            if ratio > max_rejected_fraction:
                failed.append(name + "_reject_fraction")
    return {"status": "FAIL" if failed else "PASS",
            "breaches": failed, "event": event}


def append_event(root: str | Path, event: dict, *,
                 max_seconds: float = 600,
                 max_rejected_fraction: float = 0.0) -> dict:
    result = evaluate(event, max_seconds=max_seconds,
                      max_rejected_fraction=max_rejected_fraction)
    entry = {"recorded_utc": datetime.now(timezone.utc).isoformat(), **result}
    target = Path(root) / "monitoring" / "events.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")
    return entry


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="outputs/phase4_demo")
    p.add_argument("--stage", default="validate")
    p.add_argument("--max-seconds", type=float, default=600)
    p.add_argument("--max-reject-fraction", type=float, default=0.0)
    args = p.parse_args()
    root = Path(args.output_dir)
    latest = root / "monitoring" / f"{args.stage}_latest.json"
    source = json.loads(latest.read_text())
    event = {"stage": source["stage"], "status": source["status"],
             "elapsed_seconds": source.get("elapsed_seconds", 0),
             "source": "phase4_local"}
    result = source.get("result", {})
    if isinstance(result, dict):
        metrics = result.get("metrics", {})
        if isinstance(metrics, dict):
            for name in ("claims", "encounters"):
                for suffix in ("rows", "rejected_rows"):
                    key = f"{name}_{suffix}"
                    if key in metrics:
                        event[f"{name}_{'valid' if suffix == 'rows' else 'rejected'}"] = metrics[key]
    recorded = append_event(root, event, max_seconds=args.max_seconds,
                            max_rejected_fraction=args.max_reject_fraction)
    print(json.dumps(recorded, indent=2))
    if recorded["status"] != "PASS":
        raise SystemExit("Monitoring thresholds breached")


if __name__ == "__main__":
    main()
