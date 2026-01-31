# E-Commerce Data Platform (AWS Glue + Airflow)
Welcome to event driven E-ecommerce data platform repository. 
This project demomnstrates handling of highly messy and multi format data that contains schema drifts, duplicate records, dirty values, partition records, corrupt files and still deliver analytics ready data to load it redshift and automate using airflow

---
## Architecture
This project follows Medallion Architecture Bronze, Silver, and Gold layers and automated using Airflow:

<img src="docs/architecture/full_architecture.drawio.png" width="250"/>

---

1. **Raw Ingestion**: This validates files for format and corrupt files and ensures bad data does not break downstream processing.
2. **Bronze layer**: Stores still raw but validated data.
3. **Silver layer**: This is where transformation is applied: cleansing, standardization, and normalization processes to prepare data for analysis.
4. **Gold layer**: This layer houses business-ready data modeled into a star schema required for reporting and analytics

---
##Project Overview

This Project involves:
 1. **Data Architecture**: Designing a Modern Data LakeHouse using Medallion Arichitecture: **Bronze**, **Silver**, **Gold** layers.
 2. **ETL Pipeline**: Extractiong, transforming, and loading data from s3 to into redshift serverless.
 3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries and mart for business ready metrics.

## Tech stacks used:

1. Syntheic datasets creation locally ubuntu, venv, pyspark, faker, tqdm.
2. Airflow for automating.
3. Pyspark for transformation logic.
4. AWS: s3 as data lake, IAM for permission and security, GlueJob for running spark job, redshift for datawarehousing and analytical queries

## Project Requirements 

#### Objective
Develop a modern data lakehouse using Airflow, Spark, Glue, s3 as data lake, Redshift to consolidate events, transactions and users data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Locally created datasets: events, transaction, and users. It is highly messy, multi-format(.csv, .csv.gz .json, .ndjson, .ndjson.gz, .txt, .gz) and corrupt data.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine three datasets into friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop pyspark-based, aws hosted and airflow automated to deliver detailed insights into:

- **User Activiy**
- **Daily Revenue**
- **User Life-Time-Value**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

