## Section 9 — Error Handling & Quarantine Strategy (Enterprise Grade)

This section documents how failures and bad data are handled so the pipeline remains reliable, debuggable, auditable, and safe for analytics consumption.

---

### 9.1 Failure Philosophy
The pipeline follows three core principles:

- **Fail fast, fail explicitly**  
  Critical correctness checks (schema enforcement, required fields, empty outputs) raise errors that immediately stop execution.

- **Isolate, don’t delete**  
  Bad data is quarantined, not erased. This preserves auditability and enables controlled recovery.

- **Automated stop on failure**  
  Any failed Glue job or validation step causes the Airflow DAG to fail, preventing downstream contamination.

This mirrors how production data platforms are operated in real companies.

---

### 9.2 Error Categories & Handling Paths

**1️ File-level corruption (RAW → BRONZE ingestion)**  
Detected by:
- try_read() failures
- S3 read exceptions

Action:
- File is copied to:
  s3://ecom-p3-quarantine/{dataset}/
- Original RAW file is deleted
- Ingestion continues for remaining files

---

**2️ Invalid or unsupported file format (ingestion)**  
Detected by:
- valid_format() checks

Action:
- File is moved to quarantine
- Reason is logged in Airflow task logs

---

**3️ Parse / structural failures (Bronze reader)**  
Detected by:
- Reader-level parsing failures
- Schema casting failures during normalization

Action:
- Affected rows are quarantined via custom logic
- Remaining valid rows continue through the pipeline

 No Glue-native badRecordsPath is used — all handling is explicit and code-controlled.

---

**4️ Row-level DQ failures (Bronze → Silver / Silver → Gold)**  
Examples:
- Missing primary keys
- Invalid timestamps
- Negative or impossible numeric values
- Business-rule violations

Action:
- Rows are written to:
  s3://ecom-p3-quarantine/<dataset>/
- Valid rows proceed downstream

---

**5️ Strict correctness failures**  
Examples:
- Zero valid rows after normalization
- All rows quarantined
- Required-field enforcement fails

Action:
- STRICT_FAIL exception raised
- Glue job aborts
- Airflow marks DAG as failed
- No downstream execution occurs

---

**6️ Transient infrastructure errors**  
Examples:
- Glue executor issues
- Temporary S3 or Redshift API failures

Action:
- Handled via Airflow retry configuration
- If retries are exhausted, the job fails and diagnostic logs remain available

---

### 9.3 Quarantine Data Layout & Traceability
Quarantined data is stored per dataset, not deleted.

s3://ecom-p3-quarantine/
- events/
- users/
- transactions/

Each quarantined dataset includes:
- The offending records
- Dataset context
- Job execution logs (via CloudWatch)
- Associated metrics snapshots (from S3 metrics)

This provides sufficient information for root-cause analysis and recovery, without overengineering metadata structures.

---

### 9.4 Recovery & Replay Strategy
The pipeline supports manual, controlled recovery:
- Quarantined rows remain available for inspection
- Engineers can:
  1. Fix schema or validation logic
  2. Re-run Glue jobs on corrected Bronze data
  3. Optionally replay files by re-ingesting from RAW

Unrecoverable files (e.g., corrupt binaries) can be retained for audit and later archived via lifecycle rules.

---

### 9.5 Observability & Debugging
Visibility is provided through:
- Airflow task logs (ingestion, orchestration)
- Glue job logs (Spark execution, failures)
- S3 metrics snapshots
- CloudWatch Glue logs

Together, these provide end-to-end traceability across the pipeline.

---

### 9.6 Retention & Cost Control
Recommended lifecycle policies:
- Quarantine data: retain ~30 days
- Metrics & logs: retain 90–180 days
- Large corrupt files: optionally archive to Glacier after investigation

This balances audit needs with cost control.

---

### 9.7 Security & Access Control
- Quarantine S3 prefixes are restricted to platform engineers
- Access is controlled via IAM roles
- Sensitive data is not exposed to analytics or BI consumers

This aligns with enterprise security expectations.
