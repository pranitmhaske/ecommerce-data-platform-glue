## 6. Silver → Gold Glue Job (Deep Dive)
Dimensional modeling, fact table engineering, stable schema enforcement, and business-ready marts.  
This is the layer where your pipeline becomes analytics-grade.  
Silver → Gold is what converts cleaned operational data into facts, dims, and marts that BI tools and analysts use.

---

### 6.1 Purpose of the Silver → Gold Job
This Glue job:
- Joins cleaned Silver tables
- Builds dimensional models
- Builds fact tables for events & transactions
- Builds business metrics & aggregations (marts)
- Enforces stable schemas for downstream BI
- Handles timestamp cleanup, numeric casting, SCD behavior
- Guarantees partitioned, optimized output

This is your core business logic layer.

---

### 6.2 Gold Model Structure
Your Gold layer has three zones:

**DIM tables**
- dim_users
- dim_country
- dim_status
- dim_date

**FACT tables**
- fact_events
- fact_transactions
- fact_user_activity

**MART tables**
- daily_revenue
- daily_active_users
- user_ltv

This mirrors real e-commerce analytical warehouses.

---

### 6.3 Loading the Silver Inputs
The job reads Silver using:
events = spark.read.parquet(f"{SILVER_BASE}/events")  
users = spark.read.parquet(f"{SILVER_BASE}/users")  
transactions = spark.read.parquet(f"{SILVER_BASE}/transactions")

Before transformation:
- Void/unknown types sanitized  
  Many real datasets produce “void” type columns.  
  Your code replaces them with string(null) columns to stabilize schema.
- DQ check for event_id  
  This ensures events table integrity before building facts.

---

### 6.4 Dimensional Tables (DIMs)

**dim_users**
- SCD Type-1 (from Bronze → Silver step)
- Kept at one record per user_id
- Includes: name, city, email, phone, age, updated_at

**dim_country**
- Extracted as unique list of countries from events

**dim_status**
- Unique statuses from transactions

**dim_date**
A comprehensive date dimension generated from both events and transactions:
- date
- year
- month
- day
- day_of_week
- week_of_year
- month_name

This table is essential for BI calendar filtering.

---

### 6.5 Fact Tables (FACTs)

**fact_events**
Combines:
- event fields
- broadcast-joined user attributes
- strong timestamp sanitation
- stable schema enforcement via:
spark.createDataFrame(fact_events.rdd, GOLD_FACT_EVENTS_SCHEMA)

This enforces:
- event_id
- user_id
- ts (timestamp)
- event_date
- country
- user_city
- user_age
- user_email

Why this is enterprise-grade:  
Stable schemas prevent BI tools from breaking due to:
- missing fields
- type drift
- inconsistent null behavior

---

**fact_transactions**
Similar approach:
- Join transactions + user profile
- Sanitize timestamps
- Cast numerics properly
- Generate tx_date
- Apply FACT_TX_SCHEMA for consistency

This is mandatory for Redshift downstream ingestion.

---

**fact_user_activity**
A daily aggregations fact:
- events_count per user per day
- transactions_count
- total_revenue
- avg_transaction_amount

This is a key behavior analytics table.

---

### 6.6 Business Aggregation Marts (MARTs)

**daily_revenue**
Aggregates:
- total revenue
- transaction count
- delivered order count
- average order value

Strict schema enforced via DAILY_REVENUE_SCHEMA.

---

**daily_active_users**
Union of:
- event-active users
- transaction-active users

Builds:
- event_active_users
- tx_active_users
- total_active_users

---

**user_ltv**
Lifetime Value table built from:
- first event
- last event
- transaction totals

This is a critical BI metric.

---

### 6.7 Stable Schema Enforcement
Every Gold fact/dim/mart uses:
spark.createDataFrame(df.rdd, <SCHEMA>)

This enforces:
- Correct column ordering
- Correct data types
- Required BI compatibility
- 100% schema consistency across partitions and runs

This is the hallmark of a production-star, enterprise-grade data warehouse.

---

### 6.8 Performance Engineering
Your job includes:
- Broadcast joins for dimension lookups
- Coalesce before writing to reduce file counts
- PartitionOverwriteMode = dynamic
- Adaptive execution enabled

These reduce cost and speed up Glue execution.

---

### 6.9 Output Guarantees
Gold output is:
- Strictly typed
- Partitioned appropriately
- BI-ready
- Redshift-ready
- Schema-stable

This ensures that downstream systems don’t break due to drift.
