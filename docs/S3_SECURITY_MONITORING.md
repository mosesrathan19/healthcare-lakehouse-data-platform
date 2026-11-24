# S3 Integration + Production-Readiness Monitoring and Security

This is a **cost-gated portfolio extension**, not a claim of production certification.
All data are synthetic; do not upload PHI or real medical records.

## Status

| Capability | Implemented | Actually deployed |
|---|---|---|
| Databricks Unity Catalog managed Delta | Yes | Yes: 10K synthetic claims, nine tables |
| AWS S3 Parquet uploader | Yes, dry-run default | **No** |
| S3 security preflight | Yes, read-only and opt-in | **No AWS audit performed** |
| Airflow quality orchestration | Yes | Yes, previously three tasks passed locally |
| Airflow monitoring fourth task | Yes | **Not yet rerun** |
| Sanitized JSONL monitoring and thresholds | Yes | Code committed; local execution pending |
| CloudWatch alarms / notification routing | No | No |
| CloudTrail S3 object-access auditing | No | No |
| Enterprise IAM review, compliance certification | No | No |

## A. Safe S3 integration workflow

### 1. Cost checks before creating anything

Check the AWS account and region, S3 pricing, available credits/Free Tier, and set an **AWS Budget actual-cost alert**, recognizing that budgets are **alerts, not hard spend caps**. For maximum cost avoidance, **stop after the local dry run** and keep Databricks managed storage as your cloud demonstration.

If you opt into AWS later, create a dedicated S3 bucket in one region, with all **Block Public Access** settings, **Bucket owner enforced** ownership, and default encryption. All new S3 uploads receive SSE-S3 by default, but confirm your bucket settings. Turn on versioning only if you understand storage impact; noncurrent object versions are billed. Use a least-privilege IAM identity or SSO credentials. Do not create AWS access keys for use inside notebooks.

References: [S3 security](https://aws.amazon.com/s3/security/), [S3 pricing](https://aws.amazon.com/s3/pricing/), [AWS Budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html).

### 2. No-AWS preview on your MacBook

From repository root, first generate a **synthetic demo** (if outputs/demo does not already exist), using your local PySpark environment:

```bash
python -m src.cli --scale demo --output-dir outputs/demo
```

Preview upload **without contacting AWS**:

```bash
python -m src.s3_upload \
  --root outputs/demo \
  --bucket YOUR-PRIVATE-BUCKET \
  --prefix healthcare-demo/phase5
```

The default upload limit is **100 MiB** and there is no network request without `--execute`. Print the manifest, inspect expected files and total bytes.

### 3. Optional read-only security audit

With an existing bucket and AWS CLI/boto3 credentials available:

```bash
pip install boto3
python -m src.s3_security --bucket YOUR-PRIVATE-BUCKET
```

Above is a no-network preview. To query AWS **read-only**:

```bash
python -m src.s3_security --bucket YOUR-PRIVATE-BUCKET --execute
```

The script checks bucket accessibility, all four public-access block flags, bucket default encryption, location and versioning; it also reports account identity if STS is available. Unknown access/permissions **fail closed** instead of asserting a bucket is secure. **Note:** this does not inspect bucket-policy effective access, account-level blocks, TLS enforcement, organization policies, object permissions, or prove compliance.

### 4. Optional upload (this can incur charges)

**Only if you choose to use AWS**, verify your AWS account/identity and billing. Then:

```bash
python -m src.s3_upload \
  --root outputs/demo \
  --bucket YOUR-PRIVATE-BUCKET \
  --prefix healthcare-demo/phase5 \
  --execute
```

This uploads only synthetic local Bronze/Silver/Gold Parquet. Check S3 objects in console; remove them when finished. The uploader does **not** give Databricks access automatically.

### 5. Databricks external-location integration (separate, optional)

The previous successful Databricks run used **Unity Catalog managed storage**, not this bucket. Accessing your own S3 bucket from Databricks normally requires an AWS IAM role, Unity Catalog **storage credential** and **external location**, plus permissions and a supported workspace edition. Free Edition may not provide the necessary capabilities. Do **not** embed AWS secrets in a notebook.

Follow the [official Databricks S3 external-location procedure](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3). Avoid provisioning a paid workspace solely for this demonstration.

## B. Local monitoring and quality operations

The established Phase 4 Airflow DAG had three successful tasks. It now has **four** tasks:

```text
generate_synthetic_bronze
    → build_silver_and_gold
    → run_quality_gates
    → record_monitoring_metrics
```

The fourth task calls `src.observability` after validation, extracts **only whitelisted, non-PHI metrics**, appends a timestamped JSONL event to `outputs/phase4_airflow_demo/monitoring/events.jsonl`, and exits nonzero if stage status, run duration, or reject-rate SLOs fail. A failed earlier quality task stops downstream monitoring; the failure remains visible in Airflow task state/logs. This is **not an independent alerting system**. A real monitoring system would also capture failures from upstream tasks with separate alert callbacks/notifications.

### Running the updated DAG on MacBook

From Mac Terminal:

```bash
cd ~/Documents/healthcare-lakehouse-data-platform
git pull --ff-only
cd airflow
docker compose up -d
```

Open http://localhost:8080 and manually trigger `healthcare_phase4_quality_pipeline`. Check all **four** tasks. You do not need to rebuild the image for this new Python source file because the project is bind-mounted inside the container.

After the run, from repository root:

```bash
cat outputs/phase4_airflow_demo/monitoring/events.jsonl
```

The JSONL file is intentionally in ignored `outputs/`, so operational metadata are not committed to GitHub.

### Local checks without AWS

```bash
python -m pytest -q tests/test_security_observability.py
python -m pytest -q tests/test_phase5_cloud.py
```

The tests use fake S3 clients and require **no AWS account**. They validate security policy failures, missing permissions, no-network dry runs, failure thresholds and whitelisted logs.

## C. Production-readiness gaps

To deploy a **real** production pipeline, you would need all of the following, designed and reviewed in the chosen environment: IAM least privilege and separation of duties; Databricks Unity Catalog grants; real secret management and rotation; network/private endpoint controls; encryption in transit and at rest; S3 bucket policies; data retention/deletion; object-access logging; versioned source batch and transactional recovery; immutable audit trails; CloudWatch/Databricks alert routing; pager/on-call procedures; infrastructure-as-code; CI/CD promotion and rollback; and compliance-specific controls as applicable.

**Cost note:** AWS CloudTrail S3 object data events are not enabled by default and can incur usage charges. CloudWatch logs/alarms and notification services may also incur charges. Do not enable broad data-event collection or continually running workers merely to make the portfolio look more complete.

Official references: [CloudTrail S3 data events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-cloudtrail-logging-for-s3.html), [Databricks Unity Catalog storage security](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3).

## Honest portfolio statement

> Built and executed a synthetic healthcare lakehouse on PySpark, Airflow and Databricks, including CDC/Delta MERGE, a one-million-record local benchmark, managed Delta tables, quality gates and run monitoring. Added cost-gated AWS S3 upload tooling and read-only security preflight; actual S3 deployment and production compliance remain unverified.
