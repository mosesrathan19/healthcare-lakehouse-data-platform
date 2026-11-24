"""Phase 5 S3 uploader: safe preview by default, no bucket creation.

Only synthetic project Parquet files under bronze/silver/gold are eligible.
Uses boto3's normal AWS credential chain (no keys in repository).
"""
from __future__ import annotations
import argparse
from pathlib import Path
import re

LAYERS = ("bronze", "silver", "gold")
DEFAULT_LIMIT_BYTES = 100 * 1024 * 1024
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
PREFIX_RE = re.compile(r"^[a-zA-Z0-9_./=-]*$")


def plan_upload(root: str | Path, bucket: str, prefix: str,
                max_bytes: int = DEFAULT_LIMIT_BYTES) -> list[dict]:
    """Return sorted upload manifest; reject unexpected paths and large payloads."""
    root = Path(root).expanduser().resolve()
    if not BUCKET_RE.fullmatch(bucket) or ".." in bucket or ".." in prefix:
        raise ValueError("Invalid bucket or prefix")
    prefix = prefix.strip("/")
    if not PREFIX_RE.fullmatch(prefix) or prefix.startswith("/"):
        raise ValueError("Unsafe S3 prefix")
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    objects = []
    for layer in LAYERS:
        folder = root / layer
        if not folder.is_dir():
            raise FileNotFoundError(f"Required synthetic layer missing: {folder}")
        for file in sorted(folder.rglob("*.parquet")):
            if file.is_symlink() or not file.is_file():
                raise ValueError(f"Symlink/non-file is not allowed: {file}")
            relative = file.relative_to(root).as_posix()
            size = file.stat().st_size
            objects.append({
                "path": str(file),
                "key": f"{prefix}/{relative}" if prefix else relative,
                "bytes": size,
            })
    if not objects:
        raise ValueError("No Parquet files found")
    total = sum(x["bytes"] for x in objects)
    if total > max_bytes:
        raise ValueError(f"Upload exceeds max_bytes ({total} > {max_bytes})")
    return objects


def upload_manifest(manifest: list[dict], bucket: str, *, execute: bool = False,
                    client=None) -> dict:
    """No network/API operation unless execute=True."""
    result = {"status": "DRY_RUN" if not execute else "UPLOADED",
              "bucket": bucket, "files": len(manifest),
              "bytes": sum(x["bytes"] for x in manifest),
              "objects": [{"key": x["key"], "bytes": x["bytes"]} for x in manifest]}
    if execute:
        if client is None:
            import boto3
            client = boto3.client("s3")
        # SSE-S3, private objects. Existing bucket must already be configured.
        for item in manifest:
            client.upload_file(item["path"], bucket, item["key"],
                               ExtraArgs={"ServerSideEncryption": "AES256"})
    return result


def main():
    p = argparse.ArgumentParser(description="Preview or upload synthetic lakehouse Parquet to an EXISTING S3 bucket")
    p.add_argument("--root", default="outputs/demo")
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="healthcare-demo/phase5")
    p.add_argument("--max-mib", type=int, default=100)
    p.add_argument("--execute", action="store_true",
                   help="Opt in to actual S3 upload and charges (default: dry run)")
    args = p.parse_args()
    if args.max_mib < 1 or args.max_mib > 1024:
        p.error("--max-mib must be between 1 and 1024")
    manifest = plan_upload(args.root, args.bucket, args.prefix,
                           args.max_mib * 1024 * 1024)
    import json
    print(json.dumps(upload_manifest(manifest, args.bucket,
                                     execute=args.execute), indent=2))


if __name__ == "__main__":
    main()
