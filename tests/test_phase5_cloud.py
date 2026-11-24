"""Phase 5 cost-safety tests. No AWS or Databricks credentials required."""
from pathlib import Path
import pytest
from src.cloud_pipeline import identifiers, plan
from src.s3_upload import plan_upload, upload_manifest


def test_cloud_plan_is_small_and_requires_opt_in():
    result = plan("workspace", "healthcare_demo", "demo")
    assert result["claims"] == 10000
    assert len(result["tables"]) == 9
    assert result["table_type"] == "Unity Catalog managed"


def test_reject_unsafe_identifiers_and_large_scale():
    for catalog, schema in [("bad;DROP", "demo"), ("workspace", "bad-name"),
                            ("hive_metastore", "demo")]:
        with pytest.raises(ValueError):
            identifiers(catalog, schema)
    with pytest.raises(ValueError):
        plan("workspace", "demo", "large")


def test_s3_dry_run_never_calls_network(tmp_path):
    for layer in ("bronze", "silver", "gold"):
        target = tmp_path / layer / "claims"
        target.mkdir(parents=True)
        (target / "part-000.parquet").write_bytes(b"synthetic")
    manifest = plan_upload(tmp_path, "my-test-unique-bucket",
                           "healthcare-demo/phase5")
    class ExplodingClient:
        def upload_file(self, *args, **kwargs):
            raise AssertionError("Network was called on dry run")
    preview = upload_manifest(manifest, "my-test-unique-bucket",
                              client=ExplodingClient())
    assert preview["status"] == "DRY_RUN"
    assert preview["files"] == 3
    assert preview["bytes"] == 27
    assert all(x["key"].startswith("healthcare-demo/phase5/")
               for x in preview["objects"])


def test_s3_execute_is_explicit(tmp_path):
    for layer in ("bronze", "silver", "gold"):
        d = tmp_path / layer
        d.mkdir()
        (d / "part.parquet").write_bytes(b"fake")
    manifest = plan_upload(tmp_path, "my-test-unique-bucket", "")
    class FakeClient:
        def __init__(self):
            self.calls = []
        def upload_file(self, *args, **kwargs):
            self.calls.append((args, kwargs))
    client = FakeClient()
    result = upload_manifest(manifest, "my-test-unique-bucket",
                             execute=True, client=client)
    assert result["status"] == "UPLOADED"
    assert len(client.calls) == 3
    assert all(call[1]["ExtraArgs"]["ServerSideEncryption"] == "AES256"
               for call in client.calls)


def test_s3_guards(tmp_path):
    for layer in ("bronze", "silver", "gold"):
        d = tmp_path / layer
        d.mkdir()
        (d / "part.parquet").write_bytes(b"fake")
    with pytest.raises(ValueError):
        plan_upload(tmp_path, "bad_bucket", "ok")
    with pytest.raises(ValueError):
        plan_upload(tmp_path, "my-bucket", "../escape")
    with pytest.raises(ValueError):
        plan_upload(tmp_path, "my-bucket", "ok", max_bytes=1)
