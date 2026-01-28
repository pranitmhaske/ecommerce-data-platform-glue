# Event-Driven E-Commerce Data Platform (AWS Glue + Airflow)

## Architecture

End-to-end, event-driven data platform using Airflow, AWS Glue, S3 (Bronze/Silver/Gold), and Redshift Serverless.

<img src="docs/architecture/full_architecture.drawio.png" width="100"/>

This repository demonstrates a production-style data engineering platform designed to ingest, clean, model, and serve e-commerce data at scale.

The system is built to mirror real enterprise data platforms, focusing on reliability, data quality, and operational correctness.

Production-grade ELT analytics platform built with Airflow, AWS Glue (Spark), Amazon S3, and Redshift Serverless.

- Event-driven ingestion of untrusted data (S3 → Airflow)
- RAW → BRONZE → SILVER → GOLD lakehouse architecture
- Schema enforcement, schema drift handling, quarantine-based error isolation, SCD Type 1 dimensions
- Deterministic fact & dimension modeling for analytics
- Scales from GBs to TBs without redesign
