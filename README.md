# Event-Driven E-Commerce Data Platform (AWS)

Production-grade ELT analytics platform built with
Airflow, AWS Glue (Spark), Amazon S3, and Redshift Serverless.

- Event-driven ingestion of untrusted data (S3 → Airflow)
- RAW → BRONZE → SILVER → GOLD lakehouse architecture
- Schema enforcement, drift handling, quarantine-based error isolation, SCD TYPE-1
- Deterministic fact & dimension modeling for analytics
- Scales from GBs to TBs without redesign

Built to mirror real enterprise data platforms.
