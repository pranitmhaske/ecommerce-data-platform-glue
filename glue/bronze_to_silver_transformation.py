import sys
from pyspark import StorageLevel

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from pyspark.sql import functions as F

from utils.readers import load_bronze_data
from utils.schema_normalizer import normalize_schema, SCHEMAS as DATASET_SCHEMAS
from utils.cleaners import clean_columns
from utils.writers import write_silver_output
from utils.deduper import dedupe
from utils.quarantine import quarantine_rows, sanitize_void_columns
from utils.validator import validate
from utils.scd_merge import merge_user_history
from utils.logger import get_logger

# GLUE ARGUMENTS
args = getResolvedOptions(
    sys.argv,
    ["bronze_path", "silver_path", "quarantine_path", "metrics_path", "JOB_NAME"]
)

bronze_base = args["bronze_path"]
silver_base = args["silver_path"]
quarantine_base = args["quarantine_path"]
metrics_base = args["metrics_path"]

# SPARK / GLUE SESSION
def create_spark_session(app_name="bronze_to_silver"):
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")

    return spark, glueContext

# EMPTY CHECK 
def assert_non_empty_fast(df, step, dataset):
    if not df.take(1):
        raise RuntimeError(
            f"STRICT_FAIL: `{step}` produced 0 rows for dataset '{dataset}'"
        )
    return df

# EVENT DATE LOGIC
def add_event_date(df):
    if "ts" in df.columns:
        return df.withColumn("event_date", F.to_date("ts"))
    if "event_timestamp" in df.columns:
        return df.withColumn("event_date", F.to_date("event_timestamp"))
    if "updated_at" in df.columns:
        return df.withColumn("event_date", F.to_date("updated_at"))
    if "created_at" in df.columns:
        return df.withColumn("event_date", F.to_date("created_at"))
    return df.withColumn("event_date", F.current_date())

# DATASET PROCESSOR
def process_dataset(spark, dataset_name, bronze_path, silver_path, quarantine_base, metrics_base):
    print("\n=================================================")
    print(f" PROCESSING DATASET → {dataset_name.upper()}")
    print("=================================================\n")

    # 1) Load bronze (hard stop)
    df = load_bronze_data(spark, bronze_path)
    assert_non_empty_fast(df, "load_bronze_data", dataset_name)

    # 2) Normalize schema
    df, _ = normalize_schema(df, dataset_name, allow_quarantine=True)

    # 3) Clean
    df = clean_columns(df, dataset_name)

    # 4) Add event_date early
    df = add_event_date(df)

    # 5) Dedupe
    df = dedupe(df, dataset_name)

    # 6) Quarantine
    quarantine_path = f"{quarantine_base}/{dataset_name}"
    good, bad = quarantine_rows(df, dataset_name, quarantine_path)

    # 7) Persist GOOD rows only
    good = good.persist(StorageLevel.MEMORY_AND_DISK)
    assert_non_empty_fast(good, "post_quarantine", dataset_name)

    # 8) SCD merge (users only)
    if dataset_name == "users":
        good = merge_user_history(good)
        good = good.withColumn("event_date", F.to_date("updated_at"))
        good = sanitize_void_columns(good)

    # 9) DQ validation
    metrics_path = f"{metrics_base}/{dataset_name}"
    good = validate(good, spark, dataset_name, metrics_path)

    # 10) Schema lock
    expected_cols = DATASET_SCHEMAS.get(dataset_name, [])

    for col in expected_cols:
        if col not in good.columns:
            good = good.withColumn(col, F.lit(None).cast("string"))

    good = good.withColumn(
        "event_date",
        F.coalesce(F.col("event_date"), F.lit("1900-01-01")).cast("date")
    )

    good = good.select(*expected_cols, "event_date")

    # 11) Cast all except event_date
    for c in expected_cols:
        good = good.withColumn(c, F.col(c).cast("string"))

    # 12) Repartition by event_date
    good = good.repartition(50, "event_date")

    # 13) Write silver
    write_silver_output(good, silver_path)

    print(f"COMPLETED DATASET → {dataset_name}")

# MAIN
def main():
    spark, glueContext = create_spark_session()
    logger = get_logger("bronze_to_silver")

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    datasets = ["events", "transactions", "users"]

    try:
        for ds in datasets:
            process_dataset(
                spark,
                ds,
                f"{bronze_base}/{ds}",
                f"{silver_base}/{ds}",
                quarantine_base,
                metrics_base,
            )

    except Exception as e:
        logger.error(f"STRICT PIPELINE FAILURE → {e}")
        spark.stop()
        sys.exit(1)

    job.commit()
    spark.stop()
    print("~ALL DATASETS PROCESSED~")

# ENTRYPOINT
if __name__ == "__main__":
    main()
