# Phase 5 — Verified Databricks managed Delta demo

**Execution:** November 24, 2026 (user-reported successful run)  
**Environment:** Databricks workspace with Unity Catalog; exact compute SKU, edition, and credits used were not captured  
**Deployment type:** Unity Catalog **managed Delta tables**; **not an AWS S3 external-location deployment**

## Measured run

| Metric | Result |
|---|---:|
| Execution status | PASS |
| End-to-end cloud pipeline elapsed | **44.955 seconds** |
| Synthetic patients | 1,000 |
| Synthetic providers | 100 |
| Synthetic encounters | 10,000 |
| Synthetic claims | **10,000** |
| Reconciled billed amount | **999,750,000 cents** ($9,997,500) |
| Managed Delta tables | **9** |

**Namespace:** `workspace.healthcare_phase5_demo`

### Tables created

| Layer | Tables |
|---|---|
| Bronze | `bronze_patients`, `bronze_providers`, `bronze_encounters`, `bronze_claims` |
| Silver | `silver_patients`, `silver_providers`, `silver_encounters`, `silver_claims` |
| Gold | `gold_claims_by_status` |

The Python pipeline returned `status='PASS'`, with 10,000 claims and billed totals reconciled. The user subsequently ran the read-only Gold query, `DESCRIBE DETAIL` for `silver_claims` (confirming Delta format), and `DESCRIBE HISTORY` successfully.

## Verification queries

```sql
SELECT claim_status, claim_count, billed_cents
FROM workspace.healthcare_phase5_demo.gold_claims_by_status;

DESCRIBE DETAIL workspace.healthcare_phase5_demo.silver_claims;
DESCRIBE HISTORY workspace.healthcare_phase5_demo.silver_claims LIMIT 3;
```

## What this demonstrates

- PySpark generation and transformation of deterministic synthetic healthcare data.
- Publication into managed Unity Catalog Delta tables spanning Bronze, Silver, and Gold.
- Data correctness gates reconciling claim counts and billed amounts.
- Successful Delta metadata and transaction-history inspection.

## Scope limitations

- This is a **10K-claim Databricks demonstration**, separate from the earlier **1M-claim Google Colab benchmark**. Their execution times must **not** be interpreted as a like-for-like cloud-versus-local performance comparison.
- The pipeline was launched interactively from a notebook. No production job scheduling, continuous ingestion, multi-run history, S3 IAM integration, or paid-cloud deployment has been verified.
- The healthcare data are **entirely synthetic**, and the implementation is not HIPAA-certified.
- AWS S3 remains an **optional, unexecuted** integration. The S3 uploader defaults to dry-run.
- No price or credit consumption number is reported. Check the workspace's usage page if you want to document costs.
