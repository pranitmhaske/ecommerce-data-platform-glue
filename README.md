# Event-Driven E-Commerce Data Platform (AWS Glue + Airflow)

## Architecture

End-to-end, event-driven data platform using Airflow, AWS Glue, S3 (Bronze/Silver/Gold), and Redshift Serverless.

<img src="docs/architecture/full_architecture.drawio.png" width="250"/>

This repository demonstrates a production-style data engineering platform designed to ingest, clean, model, and serve e-commerce data at scale.

The system is built to mirror real enterprise data platforms, focusing on reliability, data quality, and operational correctness.

Production-grade ELT analytics platform built with Airflow, AWS Glue (Spark), Amazon S3, and Redshift Serverless.

- Event-driven ingestion of untrusted data (S3 → Airflow)
- RAW → BRONZE → SILVER → GOLD lakehouse architecture
- Schema enforcement, schema drift handling, quarantine-based error isolation, SCD Type 1 dimensions
- Deterministic fact & dimension modeling for analytics
- Scales from GBs to TBs without redesign

## Execution Proof (Screenshots)

This section demonstrates that the platform was executed end-to-end on AWS.

### Airflow
- DAG orchestration and successful runs
- Master orchestrator triggering downstream pipelines

### AWS Glue
- Spark ETL job executions
- CloudWatch logs for observability

### Amazon S3
- RAW / BRONZE / SILVER / GOLD bucket structure

### Amazon Redshift Serverless
- Successful COPY from S3 GOLD
- Row-count validation on dimension, fact, and mart tables

See full execution screenshots in [`docs/screenshots`](docs/screenshots).
