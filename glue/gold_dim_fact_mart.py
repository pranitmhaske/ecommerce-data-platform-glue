import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql import types as T   

from utils.logger import get_logger
from utils.rules import dq_check_not_null


def safe_timestamp(col):
    return F.when(
        F.col(col).cast("timestamp").isNotNull(),
        F.col(col).cast("timestamp")
    ).otherwise(None)


# DF FIRST → RDD FALLBACK
def enforce_schema_df(df, schema):
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df.select([f.name for f in schema.fields])


# FIXED SCHEMAS
GOLD_FACT_EVENTS_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("ts", T.TimestampType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("country", T.StringType()),
    T.StructField("user_city", T.StringType()),
    T.StructField("user_age", T.StringType()),
    T.StructField("user_email", T.StringType())
])

DAILY_REVENUE_SCHEMA = T.StructType([
    T.StructField("date", T.DateType()),
    T.StructField("transaction_count", T.LongType()),
    T.StructField("total_revenue", T.DoubleType()),
    T.StructField("avg_amount", T.DoubleType()),
    T.StructField("delivered_count", T.LongType())
])

FACT_TX_SCHEMA = T.StructType([
    T.StructField("tx_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("delivered_at", T.TimestampType()),
    T.StructField("tx_date", T.DateType()),
    T.StructField("user_city", T.StringType()),
    T.StructField("user_age", T.StringType()),
    T.StructField("user_email", T.StringType())
])


# SPARK / GLUE SESSION
def create_spark_session(app_name="silver_to_gold"):
    sc = SparkContext.getOrCreate()
    sc.setAppName(app_name)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark, glueContext


# LOAD SILVER TABLES
def load_silver_tables(spark, base_path):
    events = spark.read.parquet(f"{base_path}/events")
    users = spark.read.parquet(f"{base_path}/users")
    transactions = spark.read.parquet(f"{base_path}/transactions")

    return events, users, transactions


# DIM TABLES
def build_dim_users(users, output_path):
    users = users.withColumn("updated_at", F.to_timestamp("updated_at"))
    dim_users = (
        users.select(
            "user_id", "name", "email", "city", "phone", "age", "updated_at"
        ).dropDuplicates(["user_id"])
    )
    dim_users.write.mode("overwrite").parquet(f"{output_path}/dim_users")
    print("dim_users written")


def build_dim_country(events, output_path):
    dim_country = (
        events.select(F.col("country").alias("country"))
        .where(F.col("country").isNotNull())
        .dropDuplicates()
    )
    dim_country.write.mode("overwrite").parquet(f"{output_path}/dim_country")
    print("dim_country written")


def build_dim_status(transactions, output_path):
    dim_status = (
        transactions.select("status")
        .where(F.col("status").isNotNull())
        .dropDuplicates()
    )
    dim_status.write.mode("overwrite").parquet(f"{output_path}/dim_status")
    print("dim_status written")


def build_dim_dates(events, transactions, output_path):
    event_dates = events.select(F.to_date("ts").alias("date"))
    tx_dates = transactions.select(F.to_date("created_at").alias("date"))

    dim_date = (
        event_dates.union(tx_dates)
        .dropDuplicates()
        .withColumn("year", F.year("date").cast("int"))
        .withColumn("month", F.month("date").cast("int"))
        .withColumn("day", F.dayofmonth("date").cast("int"))
        .withColumn("day_of_week", F.date_format("date", "E").cast("string"))
        .withColumn("week_of_year", F.weekofyear("date").cast("int"))
        .withColumn("month_name", F.date_format("date", "MMMM").cast("string"))
    )

    dim_date.write.mode("overwrite").parquet(f"{output_path}/dim_date")
    print("dim_date written")


# FACT TABLES
def build_fact_events(events, users, output_path, spark):
    users_pruned = users.select("user_id", "city", "age", "email")
    events_pruned = events.select("event_id", "user_id", "ts", "country")

    events_pruned = events_pruned.withColumn("event_date", F.to_date("ts"))
    events_pruned = events_pruned.withColumn("ts", safe_timestamp("ts"))

    fact_events = (
        events_pruned.alias("e")
        .join(broadcast(users_pruned.alias("u")), "user_id", "left")
    )

    if "city" in fact_events.columns:
        fact_events = fact_events.withColumn("user_city", F.col("city"))
    else:
        fact_events = fact_events.withColumn("user_city", F.lit(None).cast("string"))

    if "age" in fact_events.columns:
        fact_events = fact_events.withColumn("user_age", F.col("age").cast("string"))
    else:
        fact_events = fact_events.withColumn("user_age", F.lit(None).cast("string"))

    if "email" in fact_events.columns:
        fact_events = fact_events.withColumn("user_email", F.col("email"))
    else:
        fact_events = fact_events.withColumn("user_email", F.lit(None).cast("string"))

    expected_cols = [
        "event_id", "user_id", "ts", "event_date",
        "country", "user_city", "user_age", "user_email"
    ]

    for c in expected_cols:
        if c not in fact_events.columns:
            fact_events = fact_events.withColumn(c, F.lit(None).cast("string"))

    fact_events = fact_events.select(expected_cols)

    fact_events = fact_events \
        .withColumn("ts", F.to_timestamp("ts")) \
        .withColumn("event_date", F.to_date("event_date"))

    fact_events = enforce_schema_df(fact_events, GOLD_FACT_EVENTS_SCHEMA)

    fact_events.coalesce(10).write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .parquet(f"{output_path}/fact_events")

    print("fact_events written with stable schema")


def build_fact_transactions(transactions, users, output_path, spark):
    users_pruned = users.select("user_id", "city", "age", "email")

    tx_pruned = transactions.select(
        "tx_id", "user_id", "created_at", "delivered_at", "status", "amount"
    )

    fact_tx = (
        tx_pruned.alias("t")
        .join(broadcast(users_pruned.alias("u")), "user_id", "left")
        .select(
            "tx_id",
            "user_id",
            "amount",
            "status",
            "created_at",
            "delivered_at",
            F.to_date("created_at").alias("tx_date"),
            F.col("u.city").alias("user_city"),
            F.col("age").alias("user_age"),
            F.col("email").alias("user_email")
        )
    )

    fact_tx = fact_tx.withColumn("amount", F.col("amount").cast("double"))
    fact_tx = fact_tx.withColumn("created_at", safe_timestamp("created_at"))
    fact_tx = fact_tx.withColumn("delivered_at", safe_timestamp("delivered_at"))

    fact_tx = fact_tx.select(
        "tx_id", "user_id", "amount", "status", "created_at", "delivered_at",
        "tx_date", "user_city", "user_age", "user_email"
    )

    fact_tx = enforce_schema_df(fact_tx, FACT_TX_SCHEMA)

    fact_tx.coalesce(10).write.mode("overwrite").parquet(f"{output_path}/fact_transactions")
    print("fact_transactions written")


def build_fact_user_activity(events, transactions, users, output_path):
    events_daily = (
        events.groupBy("user_id", F.to_date("ts").alias("date"))
        .agg(F.count("*").alias("events_count"))
    )

    tx_daily = (
        transactions.groupBy("user_id", F.to_date("created_at").alias("date"))
        .agg(
            F.count("*").alias("transactions_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction_amount")
        )
    )

    users_pruned = users.select("user_id")

    fact_user_activity = (
        events_daily.alias("e")
        .join(tx_daily.alias("t"), ["user_id", "date"], "full")
        .join(users_pruned.alias("u"), "user_id", "left")
        .select(
            F.coalesce("u.user_id", "e.user_id", "t.user_id").alias("user_id"),
            "date",
            F.coalesce(F.col("events_count").cast("long"), F.lit(0)).alias("events_count"),
            F.coalesce(F.col("transactions_count").cast("long"), F.lit(0)).alias("transactions_count"),
            F.coalesce(F.col("total_revenue").cast("double"), F.lit(0)).alias("total_revenue"),
            F.coalesce(F.col("avg_transaction_amount").cast("double"), F.lit(0)).alias("avg_transaction_amount")
        )
    )

    fact_user_activity.write.mode("overwrite").parquet(f"{output_path}/fact_user_activity")
    print("fact_user_activity written")


# MART TABLES
def build_daily_revenue(transactions, output_path, spark):
    daily = (
        transactions
        .groupBy(F.to_date("created_at").alias("date"))
        .agg(
            F.count("*").cast("bigint").alias("transaction_count"),
            F.sum("amount").cast("double").alias("total_revenue"),
            F.avg("amount").cast("double").alias("avg_amount"),
            F.sum(F.when(F.col("status") == "delivered", 1).otherwise(0))
                .cast("bigint").alias("delivered_count")
        )
    )

    expected_cols = [
        "date", "transaction_count", "total_revenue",
        "avg_amount", "delivered_count"
    ]

    for c in expected_cols:
        if c not in daily.columns:
            if c == "date":
                daily = daily.withColumn(c, F.lit(None).cast("date"))
            elif c in ("transaction_count", "delivered_count"):
                daily = daily.withColumn(c, F.lit(0).cast("bigint"))
            else:
                daily = daily.withColumn(c, F.lit(0.0).cast("double"))

    daily = daily.select(expected_cols)

    daily = enforce_schema_df(daily, DAILY_REVENUE_SCHEMA)

    daily.write.mode("overwrite").parquet(f"{output_path}/daily_revenue")
    print("daily_revenue written with stable schema")


def build_daily_active_users(events, transactions, output_path):
    # Deduplicate user per day at source level 
    events_users = (
        events
        .select(
            F.to_date("ts").alias("date"),
            F.col("user_id")
        )
        .dropDuplicates(["date", "user_id"])
    )

    tx_users = (
        transactions
        .select(
            F.to_date("created_at").alias("date"),
            F.col("user_id")
        )
        .dropDuplicates(["date", "user_id"])
    )

    # DAU per source
    events_dau = (
        events_users
        .groupBy("date")
        .agg(F.count("user_id").cast("bigint").alias("event_active_users"))
    )

    tx_dau = (
        tx_users
        .groupBy("date")
        .agg(F.count("user_id").cast("bigint").alias("tx_active_users"))
    )

    # Total DAU (union + dedupe once)
    total_dau = (
        events_users
        .unionByName(tx_users)
        .dropDuplicates(["date", "user_id"])
        .groupBy("date")
        .agg(F.count("user_id").cast("bigint").alias("total_active_users"))
    )

    dau = (
        total_dau
        .join(events_dau, "date", "left")
        .join(tx_dau, "date", "left")
        .withColumn("event_active_users", F.coalesce("event_active_users", F.lit(0)))
        .withColumn("tx_active_users", F.coalesce("tx_active_users", F.lit(0)))
        .select(
            "date",
            "event_active_users",
            "tx_active_users",
            "total_active_users"
        )
    )

    dau.write.mode("overwrite").parquet(f"{output_path}/daily_active_users")
    print("daily_active_users written")

def build_user_ltv(events, transactions, output_path):
    events_user = (
        events.groupBy("user_id")
        .agg(
            F.count("*").alias("event_count"),
            F.min("ts").alias("first_event"),
            F.max("ts").alias("last_event")
        )
    )

    tx_user = (
        transactions.groupBy("user_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_revenue")
        )
    )

    ltv = (
        events_user.join(tx_user, "user_id", "full")
        .withColumn("ltv", F.coalesce("total_revenue", F.lit(0)))
    )

    ltv = ltv.withColumn("first_event", safe_timestamp("first_event"))
    ltv = ltv.withColumn("last_event", safe_timestamp("last_event"))

    ltv.write.mode("overwrite").parquet(f"{output_path}/user_ltv")
    print("user_ltv written")


# MAIN
def main():
    spark, glueContext = create_spark_session()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    logger = get_logger("silver_to_gold")

    logger.info("Starting Gold layer build...")

    args = getResolvedOptions(sys.argv, ["SILVER_BASE", "GOLD_DIM", "GOLD_FACT", "GOLD_MART"])

    job = Job(glueContext)
    job.init("silver_to_gold", args)

    SILVER_BASE = args["SILVER_BASE"]
    GOLD_DIM = args["GOLD_DIM"]
    GOLD_FACT = args["GOLD_FACT"]
    GOLD_MART = args["GOLD_MART"]

    events, users, transactions = load_silver_tables(spark, SILVER_BASE)

    for c, t in events.dtypes:
        if t == "void":
            events = events.withColumn(c, F.lit(None).cast("string"))

    for c, t in users.dtypes:
        if t == "void":
            users = users.withColumn(c, F.lit(None).cast("string"))

    for c, t in transactions.dtypes:
        if t == "void":
            transactions = transactions.withColumn(c, F.lit(None).cast("string"))

    print("EVENTS SCHEMA:", events.dtypes)
    print("USERS SCHEMA:", users.dtypes)
    print("TRANSACTIONS SCHEMA:", transactions.dtypes)

    ok, bad = dq_check_not_null(events, "event_id")
    logger.info(f"DQ check event_id null: ok={ok}, bad={bad}")

    build_dim_users(users, GOLD_DIM)
    build_dim_country(events, GOLD_DIM)
    build_dim_status(transactions, GOLD_DIM)
    build_dim_dates(events, transactions, GOLD_DIM)

    build_fact_events(events, users, GOLD_FACT, spark)
    build_fact_transactions(transactions, users, GOLD_FACT, spark)
    build_fact_user_activity(events, transactions, users, GOLD_FACT)

    build_daily_revenue(transactions, GOLD_MART, spark)
    build_daily_active_users(events, transactions, GOLD_MART)
    build_user_ltv(events, transactions, GOLD_MART)

    job.commit()
    spark.stop()


if __name__ == "__main__":
    main()
