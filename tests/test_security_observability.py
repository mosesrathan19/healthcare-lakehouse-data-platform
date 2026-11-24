"""Offline tests for monitoring and read-only S3 audit. No AWS credentials needed."""
import json
import pytest
from src.observability import append_event, evaluate, sanitize
from src.s3_security import assess_bucket


class FakeS3:
    def __init__(self, secure=True):
        self.secure = secure
        self.calls = []

    def head_bucket(self, **kwargs):
        self.calls.append("head_bucket")
        return {}

    def get_public_access_block(self, **kwargs):
        self.calls.append("get_public_access_block")
        return {"PublicAccessBlockConfiguration": {
            "BlockPublicAcls": self.secure,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }}

    def get_bucket_versioning(self, **kwargs):
        self.calls.append("get_bucket_versioning")
        return {"Status": "Enabled"}

    def get_bucket_encryption(self, **kwargs):
        self.calls.append("get_bucket_encryption")
        return {"ServerSideEncryptionConfiguration": {"Rules": [
            {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
        ]}}

    def get_bucket_location(self, **kwargs):
        self.calls.append("get_bucket_location")
        return {"LocationConstraint": "us-west-2"}


def test_secure_bucket_audit_read_only():
    client = FakeS3()
    report = assess_bucket(client, "my-test-bucket")
    assert report["status"] == "PASS"
    assert not any(name.startswith(("put_", "create_", "delete_"))
                   for name in client.calls)


def test_public_access_config_blocks_preflight():
    report = assess_bucket(FakeS3(False), "my-test-bucket")
    assert report["status"] == "FAIL"
    assert "bucket_block_public_access" in report["blocking_checks"]


def test_missing_permissions_fail_closed():
    class MissingPermissions(FakeS3):
        def get_bucket_encryption(self, **kwargs):
            raise PermissionError("Not authorized")
    report = assess_bucket(MissingPermissions(), "my-test-bucket")
    assert report["status"] == "FAIL"
    assert "bucket_default_encryption" in report["blocking_checks"]


def test_monitoring_records_sanitized_fields(tmp_path):
    event = {"stage": "validate", "status": "SUCCESS",
             "elapsed_seconds": 3.5, "claims_valid": 10000,
             "claims_rejected": 0, "patient_name": "DO_NOT_LOG"}
    result = append_event(tmp_path, event, max_seconds=10)
    assert result["status"] == "PASS"
    line = (tmp_path / "monitoring" / "events.jsonl").read_text()
    assert "DO_NOT_LOG" not in line
    assert json.loads(line)["event"]["claims_valid"] == 10000


def test_monitoring_flags_failures_and_slowness():
    event = {"stage": "validate", "status": "FAILED",
             "elapsed_seconds": 15, "claims_valid": 9,
             "claims_rejected": 1}
    report = evaluate(event, max_seconds=10, max_rejected_fraction=0.05)
    assert report["status"] == "FAIL"
    assert set(report["breaches"]) == {
        "stage_failed", "duration_threshold", "claims_reject_fraction"
    }


def test_monitoring_rejects_invalid_metrics():
    with pytest.raises(ValueError):
        sanitize({"stage": "x", "status": "SUCCESS", "elapsed_seconds": -1})
