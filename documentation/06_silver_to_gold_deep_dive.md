# 6. Silver → Gold Glue Job (Deep Dive)

**Dimensional modeling, fact engineering, schema stability, and analytics-ready marts**

The Silver → Gold Glue job is the core business logic layer of the pipeline.  
It converts cleaned and normalized Silver data into analytics-grade dimensional models, fact tables, and business marts consumed by BI tools and downstream systems.

This layer mirrors how real-world production data warehouses are designed and operated.

---

## 6.1 Purpose of the Silver → Gold Job

This Glue Spark job performs the following responsibilities:

- Reads cleaned Silver datasets
- Builds dimensional tables (DIM)
- Builds transactional and behavioral fact tables (FACT)
- Builds aggregated business metrics (MART)
- Enforces stable, deterministic schemas for BI and Redshift
- Sanitizes timestamps and numeric fields
- Applies optimized join strategies and execution settings
- Produces analytics-ready, schema-safe Gold outputs

This is the layer where the pipeline becomes analytics-grade.

---

## 6.2 Gold Layer Model Structure

The Gold layer is logically divided into three zones:

### DIM Tables
- dim_users  
- dim_country  
- dim_status  
- dim_date  

### FACT Tables
- fact_events  
- fact_transactions  
- fact_user_activity  

### MART Tables
- daily_revenue  
- daily_active_users  
- user_ltv  

This structure mirrors real e-commerce analytical warehouses.

---

## 6.3 Loading and Validating Silver Inputs

Silver data is loaded directly from S3:

```python
events = spark.read.parquet(f"{SILVER_BASE}/events")
users = spark.read.parquet(f"{SILVER_BASE}/users")
transactions = spark.read.parquet(f"{SILVER_BASE}/transactions")
```

### Pre-Transformation Safeguards

Before any modeling:

* Void or unknown Spark column types are sanitized  
  Columns with `void` type are replaced with `string(null)` to stabilize schemas.

* Data quality validation on `event_id`  
  A not-null check ensures event integrity before fact generation.

These safeguards prevent downstream schema breakage.

---

## 6.4 Dimensional Tables (DIM)

### dim_users

* One record per user_id
* SCD Type-1 behavior already resolved in Bronze → Silver
* Fields:
  * user_id
  * name
  * email
  * city
  * phone
  * age
  * updated_at

### dim_country

* Unique list of countries derived from events

### dim_status

* Unique transaction statuses

### dim_date

A full calendar dimension generated from both events and transactions:

* date
* year
* month
* day
* day_of_week
* week_of_year
* month_name

This enables proper time-based analysis in BI tools.

---r

## 6.5 Fact Tables (FACT)

### fact_events

Built by joining:

* Event-level activity
* User attributes via broadcast join

Key characteristics:

* Strong timestamp sanitation
* Explicit column selection
* Stable schema enforcement
* Event-level granularity

Schema enforced:

* event_id
* user_id
* ts
* event_date
* country
* user_city
* user_age
* user_email

Stable schemas prevent BI tools from breaking due to drift.

---

### fact_transactions

Built by joining:

* Transactions
* User attributes

Features:

* Numeric casting for monetary fields
* Timestamp sanitation
* Derived tx_date
* Explicit schema enforcement

This table is Redshift-ready and supports revenue and order analytics.

---

### fact_user_activity

Daily user-level behavioral fact table:

* events_count
* transactions_count
* total_revenue
* avg_transaction_amount

Used for engagement and retention analysis.

---

## 6.6 Business Aggregation Marts (MART)

### daily_revenue

Daily revenue metrics:

* transaction_count
* total_revenue
* avg_amount
* delivered_count

Schema enforced for BI compatibility.

---

### daily_active_users

Computed using:

* Event activity
* Transaction activity
* Union + deduplication

Outputs:

* event_active_users
* tx_active_users
* total_active_users

---

### user_ltv

Lifetime value metrics per user:

* first_event
* last_event
* total revenue (LTV)

Critical for customer analytics.

---

## 6.7 Stable Schema Enforcement Strategy

All Gold facts and marts use explicit schema enforcement.

Strategy:

* DF-first casting for performance
* RDD fallback for malformed input

This guarantees:

* Correct column ordering
* Correct data types
* BI and Redshift compatibility
* Zero schema drift across runs

This is a core enterprise-grade design principle.

---

## 6.8 Performance Engineering

The job is optimized using:

* Broadcast joins for dimension lookups
* Adaptive Query Execution (AQE)
* Skew join handling
* Dynamic partition overwrite mode
* Controlled file counts via selective coalescing

These optimizations reduce cost and improve Glue stability.

---

## 6.9 Output Guarantees

Gold outputs are:

* Strictly typed
* Schema-stable
* Analytics-ready
* BI-safe
* Redshift-compatible

Downstream systems never break due to schema drift.
