from pyspark.sql import functions as F

def write_silver_output(df, path: str):

    # Detect partition column
    if "ts" in df.columns:
        partition_col = "ts"
    elif "updated_at" in df.columns:
        partition_col = "updated_at"
    elif "created_at" in df.columns:
        partition_col = "created_at"
    else:
        partition_col = None

    # Add event_date partition column
    if partition_col:
        df = df.withColumn(
            "event_date",
            F.to_date(
                F.coalesce(
                    F.to_timestamp(partition_col),
                    F.col(partition_col).cast("timestamp")
                )
            )
        )
        partition_by = ["event_date"]
    else:
        partition_by = []

    print(f"[WRITE] Silver output → {path}")
    print(f"[WRITE] Partition columns → {partition_by}")

    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(path)

    print("[WRITE] ✔ Silver write completed successfully")

