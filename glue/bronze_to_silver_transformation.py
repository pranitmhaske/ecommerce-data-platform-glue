import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import StorageLevel
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from utils.readers import load_bronze_data
from utils.schema_normalizer import normalize_schema, SCHEMAS as DATASET_SCHEMAS
from utils.quarantine import quarantine_rows, sanitize_void_columns
from utils.validator import validate
from utils.cleaners import clean_columns
from utils.writers import write_silver_output
from utils.deduper import dedupe
from utils.scd_merge import merge_user_history
from utils.logger import get_logger


# GLUE ARGUMENTS
args = getResolvedOptions(
    sys.argv,
    ["bronze_path", "silver_path", "quarantine_path", "metrics_path", "JOB_NAME"]
)

bronze_base = args["bronze_path"] # raw data lands here
silver_base = args["silver_path"] # this is where cleaned data lands
quarantine_base = args["quarantine_path"] # corrupt or invalid quarantined here
metrics_base = args["metrics_path"] # data quality metrics calculated and store here


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


# fails for zero output
def fails_if_zero_output(df, step, dataset):
    if not df.take(1):
        raise RuntimeError(f"The {dataset} will fail if it produced zero rows for: {step}")
    return df


# event_date for partition logic
def add_event_date(df):
    # Most datasets use ts; users are the exception
    if "ts" in df.columns:
        return df.withColumn("event_date", F.to_date("ts"))

    # Some older transaction dumps use event_timestamp
    if "event_timestamp" in df.columns:
        return df.withColumn("event_date", F.to_date("event_timestamp"))

    if "updated_at" in df.columns:
        return df.withColumn("event_date", F.to_date("updated_at"))

    # Fallback only for malformed records
    return df.withColumn("event_date", F.current_date())

# a systematic flow of processing datasets
def process_dataset(spark, dataset_name, bronze_path, silver_path, quarantine_base, metrics_base):
    print(f"The dataset processing start : {dataset_name.upper()}")

    # this loads bronze data
    df = load_bronze_data(spark, bronze_path)
    fails_if_zero_output(df, "load_bronze_data", dataset_name)

    # normalize schema to canonical definition
    df, _ = normalize_schema(df, dataset_name, allow_quarantine=True)

    # cleans columns issues like space, dirty values, timestamps
    df = clean_columns(df, dataset_name)

    # event_date must exist early to prevent 
    df = add_event_date(df)

    # removes duplicate otherwise may end up quarantining good rows
    df = dedupe(df, dataset_name)

    # quarantine bad rows into quarantine bucket, and good data continues to flow
    quarantine_path = f"{quarantine_base}/{dataset_name}"
    good, bad = quarantine_rows(df, dataset_name, quarantine_path)

    good = good.persist(StorageLevel.MEMORY_AND_DISK)
    fails_if_zero_output(good, "post_quarantine", dataset_name)

    # scd is timestamp only because of no tie breaker column
    if dataset_name == "users":
        good = merge_user_history(good)
        # users are SCD-based; event_date tracks last update
        good = good.withColumn("event_date", F.to_date("updated_at"))
        good = sanitize_void_columns(good)

    # data quality metrics like rows count, null rate are stored here
    metrics_path = f"{metrics_base}/{dataset_name}"
    good = validate(good, spark, dataset_name, metrics_path)

    # locking schema guarantees that downstream system stay stable
    expected_cols = DATASET_SCHEMAS.get(dataset_name, [])

    for col in expected_cols:
        if col not in good.columns:
            good = good.withColumn(col, F.lit(None).cast("string"))

    good = good.withColumn(
        "event_date",
        F.coalesce(F.col("event_date"), F.lit("1900-01-01")).cast("date"),
    )

    good = good.select(*expected_cols, "event_date")

    # Cast all to strings except partition columns
    for c in expected_cols:
        good = good.withColumn(c, F.col(c).cast("string"))

    # Repartition by event_date
    good = good.repartition(50, "event_date")

    # finally, Write silver output
    write_silver_output(good, silver_path)

    print(f"COMPLETED PROCESSING DATASET : {dataset_name}")

# MAIN
def main():
    spark, glueContext = create_spark_session()
    logger = get_logger("bronze_to_silver")

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    datasets = ["events", "transactions", "users"]

    try:
        for ds in datasets:
            process_dataset(spark, ds,
                f"{bronze_base}/{ds}",
                f"{silver_base}/{ds}",
                quarantine_base,
                metrics_base,
            )

    except Exception as e:
        logger.error(f"PIPELINE FAILURE : {e}")
        spark.stop()
        sys.exit(1)

    job.commit()
    spark.stop()
    print("~ALL DATASETS PROCESSED~")
if __name__ == "__main__":
    main()
