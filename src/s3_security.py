"""Opt-in, read-only AWS S3 security preflight for synthetic healthcare demo.

No resource changes are made. Missing permissions produce UNKNOWN checks and
a failing gate; no claim of security compliance is made.
"""
from __future__ import annotations

import argparse
import json
from botocore.exceptions import ClientError

REQUIRED_BLOCKS = ("BlockPublicAcls", "IgnorePublicAcls",
                   "BlockPublicPolicy", "RestrictPublicBuckets")


def assess_bucket(client, bucket: str, *, sts_client=None) -> dict:
    """Inspect an existing bucket. Only GET/HEAD API calls are made."""
    from src.s3_upload import plan_upload
    # Validate name without requiring any local Parquet data.
    import re
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or ".." in bucket:
        raise ValueError("Invalid bucket name")

    checks = []
    def record(name, status, detail):
        checks.append({"check": name, "status": status, "detail": detail})
    try:
        client.head_bucket(Bucket=bucket)
        record("bucket_accessible", "PASS", "HEAD request succeeded")
    except Exception as exc:
        record("bucket_accessible", "UNKNOWN", type(exc).__name__)
    try:
        config = client.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        good = all(config.get(field) is True for field in REQUIRED_BLOCKS)
        record("bucket_block_public_access", "PASS" if good else "FAIL",
               {field: bool(config.get(field)) for field in REQUIRED_BLOCKS})
    except Exception as exc:
        record("bucket_block_public_access", "UNKNOWN", type(exc).__name__)

    try:
        policy = client.get_bucket_versioning(Bucket=bucket)
        record("bucket_versioning", "PASS" if policy.get("Status") == "Enabled" else "WARN",
               policy.get("Status", "Not enabled"))
    except Exception as exc:
        record("bucket_versioning", "UNKNOWN", type(exc).__name__)

    try:
        response = client.get_bucket_encryption(Bucket=bucket)
        rules = response["ServerSideEncryptionConfiguration"]["Rules"]
        algorithms = [rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] for rule in rules]
        record("bucket_default_encryption",
               "PASS" if algorithms and all(x in ("AES256", "aws:kms", "aws:kms:dsse") for x in algorithms) else "FAIL",
               algorithms)
    except Exception as exc:
        record("bucket_default_encryption", "UNKNOWN", type(exc).__name__)

    try:
        r = client.get_bucket_location(Bucket=bucket)
        region = r.get("LocationConstraint") or "us-east-1"
        record("bucket_region", "PASS", region)
    except Exception as exc:
        record("bucket_region", "UNKNOWN", type(exc).__name__)

    if sts_client is not None:
        try:
            r = sts_client.get_caller_identity()
            record("aws_identity", "PASS", {"account": r.get("Account"), "arn": r.get("Arn")})
        except Exception as exc:
            record("aws_identity", "UNKNOWN", type(exc).__name__)
    fail = [c["check"] for c in checks if c["status"] in ("FAIL", "UNKNOWN")]
    return {"bucket": bucket, "status": "FAIL" if fail else "PASS",
            "checks": checks, "blocking_checks": fail,
            "note": "Read-only configuration audit; does not prove effective permissions, HIPAA compliance, or account budget."}


def main():
    parser = argparse.ArgumentParser(description="Read-only check of an existing S3 bucket")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--execute", action="store_true",
                        help="Opt in to read-only AWS API calls; without flag, no AWS calls")
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({"status": "DRY_RUN", "bucket": args.bucket,
                          "message": "No AWS calls. Use --execute for read-only S3 security preflight."}, indent=2))
        return
    import boto3
    result = assess_bucket(boto3.client("s3"), args.bucket, sts_client=boto3.client("sts"))
    print(json.dumps(result, indent=2))
    if result["status"] != "PASS":
        raise SystemExit("Security preflight blocked: review failed/unknown checks")


if __name__ == "__main__":
    main()
