## Section 5 — Bronze → Silver Glue Job (Deep Dive)
**schema governance, data quality, and cleaning layer**

---

### **5.1 Purpose of the Bronze → Silver Job**
This Glue Spark job transforms raw, multi-format **Bronze** data into a clean, validated, schema-consistent **Silver** layer suitable for analytical modeling and downstream Gold transformations.

It addresses complexity, including:
- Mixed input formats (JSON, NDJSON, CSV, TXT, GZIP, Parquet)
- Schema drift across files and time
- Corrupt and invalid records
- Type inconsistencies and malformed timestamps
- Nulls and dirty string values
- Duplicate records
- Late-arriving data
- SCD-style handling for user updates
- Data quality metrics with strict fail-fast guarantees

This job represents the **core data reliability layer** of the pipeline.

---

### **5.2 Multi-Format Bronze Reader**
The `readers.py` module implements a unified Bronze ingestion layer that abstracts file-format complexity.

**Capabilities**
- Reads all supported formats:
  - Parquet  
  - JSON  
  - NDJSON  
  - CSV  
  - TXT (auto-detects JSON-lines vs delimited text)  
  - GZIP (auto-detects JSON vs CSV)
- Schema-safe union across formats (using unionByName with allowMissingColumns=True):
  - No column loss  
  - No positional mismatches  
- Canonical schema application:
  - Data is cast into dataset-specific canonical schemas via `schema_normalizer.py`

**Result:**  
All valid Bronze data—regardless of original format—enters the Silver pipeline in a consistent structural shape.

---

### **5.3 Schema Normalization & Drift Handling**
Schema normalization is handled centrally in `schema_normalizer.py`.

**Responsibilities**
- ✔ Canonical column enforcement:
  - Adds missing required columns  
  - Renames and trims columns  
  - Handles optional or unexpected fields per dataset rules
- ✔ Safe type casting:
  - Integer  
  - Double  
  - Timestamp (multiple formats supported)  
  - String
- ✔ Corrupt-record detection:
  - Rows failing structural or casting checks are flagged  
  - Routed to the quarantine pipeline
- ✔ Required-field validation:
  - `events`: event_id, user_id  
  - `users`: user_id  
  - `transactions`: tx_id, user_id  

Rows failing required-field checks are **quarantined**, not silently dropped.

---

### **5.4 Cleaning Layer**
The `clean_columns()` module applies dataset-aware cleaning logic.

**Cleaning Operations**
- ✔ Null normalization:
  - Handles `""`, `" "`, `"NULL"`, `"N/A"`, `"none"`, `"None"`, `NaN`
- ✔ String trimming & sanitization
- ✔ Numeric cleanup:
  - Removes commas  
  - Handles malformed numeric strings  
  - Enforces safe casting
- ✔ Timestamp parsing:
  - ISO  
  - Compact ISO  
  - Date-only  
  - Fallback-safe parsing
- ✔ Dataset-specific rules:
  - Separate logic paths for events, users, and transactions  
  - Designed specifically for real dataset inconsistencies

---

### **5.5 Deduplication Strategy**
Deduplication is applied after cleaning and schema normalization using partitioned window logic.

| Dataset        | Deduplication Key | Rule                  |
|----------------|-------------------|-----------------------|
| Events         | event_id          | Keep latest record    |
| Users          | user_id           | Latest updated_at     |
| Transactions   | tx_id             | Keep latest record    |

This prevents:
- Double-counted events  
- Inflated revenue  
- Duplicate user records  

---

### **5.6 Quarantine Strategy**
The pipeline implements a custom, deterministic quarantine system.

**What gets quarantined**
- Missing primary keys  
- Invalid business logic (e.g., negative amounts)  
- Impossible timestamps  
- Structurally corrupt rows  
- Schema-drifted or uncastable records  

**Storage**
s3://ecom-p3-quarantine/<dataset>/

**Why this matters**
- Silver layer contains only valid, analyzable data  
- No silent data loss  
- Full forensic visibility   

---

### **5.7 SCD-Style User Handling (Type-1 paritial)**
For the users dataset, the pipeline applies **Type-1 SCD partial**.

**Behavior**
- Merges partial updates per `user_id`
- Here it has not tie breaker column(if such a condition does occurs in production, must use ingestion timestamp)
- Produces one authoritative snapshot per user
- Rebuilds `event_date` for partition consistency

This provides a clean, current-state user dimension suitable for analytics.

---

### **5.8 Data Quality Metrics**
The validation layer emits dataset-level metrics via `validator.py`.

**Metrics Generated**
- Total row counts  
- Null rates per column  
- Dataset-specific DQ rule results  
- Quarantined row counts  

**Storage**
s3://ecom-p3-metrics/<dataset>/

Metrics can be consumed by:
- Athena  
- BI tools  
- Monitoring dashboards  
- Manual audits  

---

### **5.9 Event Date Enforcement**
Every record is guaranteed an `event_date`, derived using the following priority order:
1. `ts`  
2. `created_at`  
3. `updated_at`  
4. Fallback default  

This ensures:
- No broken partitions  
- Consistent partitioning across datasets  
- Stable downstream aggregations  

---

### **5.10 Strict Pipeline Failure Rules**
The job enforces **fail-fast semantics**.

If any critical transformation step produces **zero valid rows**, the job aborts.

This guarantees:
- No degraded data propagation  
- No silent corruption  
- Immediate visibility into upstream issues  

---

### **5.11 Output**
Silver outputs are:
- Partitioned by `event_date`
- Schema-locked
- Snappy-compressed Parquet
- Deterministic full refresh per run
- Optimized for Glue, Athena, and Redshift consumption

The `write_silver_output()` utility ensures stable, query-ready partitions.
