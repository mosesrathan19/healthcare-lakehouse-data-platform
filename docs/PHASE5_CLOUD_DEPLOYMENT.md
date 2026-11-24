# Phase 5 — AWS S3 + Databricks Delta Lake deployment

## Current status and cost policy

**Repository implementation:** safe S3 upload CLI (dry-run default), managed-Delta Databricks pipeline (preview default), guided notebook, and tests. **Neither an AWS resource nor a paid Databricks resource is created by cloning or committing this repository. Cloud execution is not yet verified.**

Use **synthetic data only**. No PHI. The quickest and lowest-risk workflow is to validate managed Delta in **Databricks Free Edition** first; an S3 bucket is optional and separate.

### Cost checklist before *any* resource is created

1. First check whether your Databricks plan says **Free Edition**, **Free Trial**, or **pay-as-you-go**. Free Edition is quota-limited and includes managed serverless compute/storage with restrictions. A free trial can transition to paid usage; don't assume it stays free.
2. If using AWS, check your Free Tier eligibility, region, payment setup, and service-specific prices at [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/). Storage, requests, and transfer may be billable even for small files.
3. In the [AWS Billing console](https://console.aws.amazon.com/costmanagement/), create a **cost budget with email notifications** before creating a bucket. Choose a low amount meaningful to you (for example **$1 actual-cost alert**). **AWS budget notifications do not impose a hard spending cap**; alerts can lag behind costs.
4. Keep all experiments manual and avoid paid clusters, scheduled recurring jobs, cross-region S3 transfers, NAT gateways, public buckets, or default auto-running tasks.
5. After each paid experiment, check current usage and delete unneeded bucket objects, external-location resources, clusters/jobs, and optional managed tables. **Deletion stops future usage but does not erase charges already incurred.**
6. Never commit AWS keys, Databricks tokens, passwords, or sensitive data. Use AWS SSO / credential providers, and Databricks Unity Catalog IAM roles, not embedded keys.

Useful current documentation:
- [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) and [limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [AWS cost budgets](https://docs.aws.amazon.com/cost-management/latest/userguide/bcm-lite-create-a-budget.html)
- [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/)
- [Unity Catalog S3 external location setup](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3)
- [Databricks Git folders](https://docs.databricks.com/aws/en/repos/)

## Path A — Databricks Managed Delta (recommended FIRST)

**No AWS account, S3 bucket, or IAM role needed** for the managed-table path if your Databricks workspace supports Unity Catalog managed storage.

1. Open [Databricks Free Edition](https://www.databricks.com/try-databricks) or your existing Databricks workspace. Confirm the plan/quotas before running.
2. In **Workspace → Git folders**, add the public repository URL:
   `https://github.com/mosesrathan19/healthcare-lakehouse-data-platform.git`
3. Open `notebooks/05_databricks_managed_delta.ipynb` from that Git folder. If the notebook import fails, ensure you're in a Git folder and that the repository root is on the Python import path (see Databricks module-import docs).
4. Use `SHOW CATALOGS` to find an allowed Unity Catalog catalog. Edit `CATALOG` in Step 2. The notebook defaults to `workspace` as an example; that name is not guaranteed.
5. Keep `SCALE="demo"` and `EXECUTE_CLOUD_WRITE=False` for the first pass. Run steps 1–3 to see the **zero-write plan**.
6. When satisfied with plan/usage, set `EXECUTE_CLOUD_WRITE=True` and run the notebook's write/verification cells once. It creates/overwrites a schema and these nine managed Delta tables: `bronze_patients`, `bronze_providers`, `bronze_encounters`, `bronze_claims`, `silver_patients`, `silver_providers`, `silver_encounters`, `silver_claims`, and `gold_claims_by_status`.
7. Confirm **10,000 valid claims** in demo mode, Gold/Silver counts and billed amount reconciliation, and `format=delta` in DESCRIBE DETAIL. Capture run output without displaying credentials.
8. Don't start a recurring schedule. A manually triggered notebook is adequate evidence for the portfolio.

**Important:** These are managed tables in workspace-managed storage. They are NOT evidence that the separate AWS S3 integration has executed. Re-running the write path overwrites the nine named tables; use a dedicated schema with nothing important in it.

### Optional manual Databricks Jobs workflow

If your plan supports Lakeflow Jobs: open **Jobs & Pipelines → Create Job**, add a **Notebook** task pointing to the Git-folder notebook, use a supported serverless compute mode, set a descriptive job name, **leave schedule unset**, and manually trigger it once. Monitor task status and usage. Note: the notebook defaults to no writes; for a job to publish tables you must deliberately change its execution flag. Job availability and billing depend on your plan. Do not create a job if Free Edition or workspace capabilities restrict it.

## Path B — AWS S3 upload (OPTIONAL; can incur charges)

This is a distinct demonstration: generate synthetic local Parquet, then safely upload it to a **preexisting** private S3 bucket. The uploader **does not** create buckets, AWS IAM users, credentials, Databricks access, or AWS budgets.

### 1. Set up a safe AWS account

- In the AWS Console: choose **one region**, make a uniquely named **S3 general-purpose bucket**, leave **Block all public access enabled**, and use default encryption. Restrict access to your account. Check S3 prices and costs first.
- Configure AWS IAM least-privilege access for **your own login** (AWS IAM Identity Center/SSO recommended), limited to the target bucket/prefix; avoid root access and hard-coded access keys. AWS CLI profiles keep credentials out of this repository.
- Create a small AWS budget alert (not a hard cap). Do not create an S3 bucket until you're comfortable with the account's billing settings.

### 2. Generate the local demo

On MacBook Terminal (from repository root, with Java/PySpark installed), or in Colab for demo data, run:

```bash
python -m src.cli --scale demo --output-dir outputs/demo
```

If you previously ran the local pipeline, you may reuse its existing `outputs/demo` directory.

### 3. Preview uploads without AWS credentials

```bash
python -m src.s3_upload --root outputs/demo --bucket YOUR-EXISTING-PRIVATE-BUCKET --prefix healthcare-demo/phase5
```

This is a **zero-network dry run**, even without `boto3`. It validates that Bronze, Silver and Gold contain Parquet files and that total upload size is **at most 100 MiB** (configurable with `--max-mib`). It prints a manifest with object keys and byte sizes.

### 4. Upload only with explicit opt-in

When you're ready, install the AWS SDK with `pip install boto3`. Configure a profile through AWS SSO or another standard credential provider. Check the active AWS identity with:

```bash
aws sts get-caller-identity
```

**Be sure it is your intended AWS account** before proceeding. Upload your synthetic files:

```bash
python -m src.s3_upload --root outputs/demo --bucket YOUR-EXISTING-PRIVATE-BUCKET --prefix healthcare-demo/phase5 --execute
```

The uploader sends private objects with SSE-S3 encryption. The configured identity must have S3 PutObject permissions to the selected prefix. The program does not delete objects, make them public, or change bucket policies. On failures, some objects may already be uploaded; inspect the bucket before retrying.

Verify the objects in the AWS S3 console (or AWS CLI). **Do not claim S3 deployment until you've confirmed the uploaded objects in your own account.**

### 5. Optional Databricks → S3 integration (requires appropriate edition and permissions)

For **direct Databricks access to S3**, upload alone is **not sufficient**. A Databricks on AWS workspace with supported Unity Catalog features must authorize S3 access via:

1. An **AWS IAM role** with narrowly scoped S3 permissions for the bucket/prefix and appropriate trust relationship.
2. A **Unity Catalog storage credential** referencing that IAM role.
3. A **Unity Catalog external location** referencing the S3 path and credential, plus Unity Catalog grants.
4. A read of the allowed external location (such as `spark.read.parquet("s3://...")`, where supported and authorized) to verify integration.

Follow the [official Databricks S3 setup](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3) and [manual external location guide](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual). Do not paste AWS access keys in notebooks, use outdated DBFS mounts, or assume Free Edition supports arbitrary external locations; check your plan first. Paid workspace/network configuration may incur additional costs.

## Test coverage

```bash
pytest -q tests/test_phase5_cloud.py
python -m compileall -q src tests
```

Tests verify safe catalog/schema identifiers, scale guardrails, S3 manifest validation, dry-run **without network calls**, size limits, and the explicit opt-in upload API with a fake S3 client. They do not require cloud credentials.

### Verified vs. unverified

- **Verified:** Source files and notebook committed to GitHub.
- **Not verified until you run:** Delta managed table creation and actual S3 upload, your account permissions, billing, and a Databricks Jobs run.
- **Not provided:** Automated AWS provisioning, live patient data, managed IAM integration, scheduled paid compute, HIPAA controls, or a hard dollar-limit guarantee.
