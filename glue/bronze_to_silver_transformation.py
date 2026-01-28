import sys

# =====================================================================
# GLUE IMPORTS
# =====================================================================
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions      # <-- added
from pyspark.context import SparkContext

# =====================================================================
# PYSPARK IMPORTS
# =====================================================================
from pyspark.sql import functions as F

# =====================================================================
# CUSTOM UTILS (CENTRALIZED LOGIC)
# =====================================================================
from utils.readers import load_bronze_data
from utils.schema_normalizer import normalize_schema, SCHEMAS as DATASET_SCHEMAS
from utils.cleaners import clean_columns
from utils.writers import write_silver_output
from utils.deduper import dedupe
from utils.quarantine import quarantine_rows, sanitize_void_columns
from utils.validator import validate
from utils.scd_merge import merge_user_history
from utils.logger import get_logger
# =====================================================================
# GLUE ARGUMENTS (NEW REQUIRED BLOCK)
# =====================================================================
args = getResolvedOptions(
    sys.argv,
    ["bronze_path", "silver_path", "quarantine_path", "metrics_path", "JOB_NAME"]
)

bronze_base = args["bronze_path"]
silver_base = args["silver_path"]
quarantine_base = args["quarantine_path"]
metrics_base = args["metrics_path"]

# =====================================================================
# SPARK / GLUE SESSION BUILDER
# =====================================================================
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

# =====================================================================
# STRICT PIPELINE STOPPER
# =====================================================================
def assert_non_empty(df, step_name, dataset_name):
    if df is None or df.count() == 0:
        raise RuntimeError(
            f"STRICT_FAIL: step `{step_name}` produced 0 rows for dataset '{dataset_name}'"
        )
    return df

# =====================================================================
# EVENT DATE LOGIC
# =====================================================================
def add_event_date(df):
    if "ts" in df.columns:
        return df.withColumn("event_date", F.to_date("ts"))
    elif "event_timestamp" in df.columns:
        return df.withColumn("event_date", F.to_date("event_timestamp"))
    elif "updated_at" in df.columns:
        return df.withColumn("event_date", F.to_date("updated_at"))
    elif "created_at" in df.columns:
        return df.withColumn("event_date", F.to_date("created_at"))
    return df.withColumn("event_date", F.current_date())

# =====================================================================
# MAIN DATASET PROCESSOR
# =====================================================================
def process_dataset(spark, dataset_name, bronze_path, silver_path, quarantine_base, metrics_base):
    print("\n=================================================")
    print(f" PROCESSING DATASET → {dataset_name.upper()}")
    print(f"BRONZE INPUT  → {bronze_path}")
    print(f"SILVER OUTPUT → {silver_path}")
    print("=================================================\n")

    # 1) load bronze
    df = load_bronze_data(spark, bronze_path)
    df = assert_non_empty(df, "load_bronze_data", dataset_name)

    # 2) normalize schema + handle drift
    df, _ = normalize_schema(df, dataset_name, allow_quarantine=True)
    df = assert_non_empty(df, "normalize_schema", dataset_name)

    # 3) clean columns
    df = clean_columns(df, dataset_name)

    # 4) add event_date early to prevent missing partitions
    df = add_event_date(df)

    # 5) strict check for empty outputs
    try:
        df = assert_non_empty(df, "clean_columns", dataset_name)
    except:
        print("WARN: corrupt rows detected, quarantining whole file")
        return

    # 6) dedupe
    df = dedupe(df, dataset_name)
    df = assert_non_empty(df, "dedupe", dataset_name)

    # 7) quarantine bad rows
    quarantine_path = f"{quarantine_base}/{dataset_name}"
    good, bad = quarantine_rows(df, dataset_name, quarantine_path)
    print("Quarantine -> good:", good.count(), "| bad:", 0 if bad is None else bad.count())

    # 8) only good rows move forward
    df = assert_non_empty(good, "quarantine_filtering", dataset_name)

    # 9) SCD merge for users only
    if dataset_name == "users":
        df = merge_user_history(df)
        df = df.withColumn("event_date", F.to_date("updated_at"))
        df = sanitize_void_columns(df)

    df = assert_non_empty(df, "scd_merge", dataset_name)

    # 10) run DQ validation
    metrics_path = f"{metrics_base}/{dataset_name}"
    df = validate(df, spark, dataset_name, metrics_path)

    # 11) schema lock + enforce event_date always exists
    print("SCHEMA BEFORE SELECT():")
    df.printSchema()

    expected = DATASET_SCHEMAS.get(dataset_name, [])
    for col in expected:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))

    # prevent Spark from dropping
    df = df.withColumn(
        "event_date",
        F.coalesce(F.col("event_date"), F.lit("1900-01-01")).cast("date")
    )

    # 12) final schema apply in fixed order
    df = df.select(*expected, "event_date")
    df = assert_non_empty(df, "schema_lock", dataset_name)

    # 13) cast all to string except event_date
    for c in expected:
        df = df.withColumn(c, F.col(c).cast("string"))

    # 14) repartition by event_date to ensure all files contain event_date
    df = df.repartition(50, "event_date")

    # 15) write silver output to S3
    write_silver_output(df, silver_path)

    print(f"COMPLETED DATASET: {dataset_name}\n")

# =====================================================================
# MAIN
# =====================================================================
def main():
    print("################  SILVER TRANSFORMATION STARTED  ################")

    spark, glueContext = create_spark_session()
    logger = get_logger("bronze_to_silver")

    # ===== Glue Job Init =====
    job_name = args["JOB_NAME"]
    job = Job(glueContext)
    job.init(job_name, args)

    datasets = ["events", "transactions", "users"]

    try:
        for ds in datasets:
            bronze_path = f"{bronze_base}/{ds}"
            silver_path = f"{silver_base}/{ds}"

            process_dataset(
                spark, ds, bronze_path, silver_path, quarantine_base, metrics_base
            )

    except Exception as e:
        logger.error(f"\n STRICT PIPELINE ERROR → {e}")
        spark.stop()
        sys.exit(1)

    # ===== Glue Job Commit =====
    job.commit()
    spark.stop()
    print("################  ALL DATASETS PROCESSED  ################\n")
