from pyspark.sql import functions as F

def dq_check_not_null(df, col):
    bad = df.filter(F.col(col).isNull()).count()
    return bad == 0, bad

def dq_check_unique(df, col):
    total = df.count()
    distinct = df.select(col).distinct().count()
    return total == distinct, total - distinct

def dq_check_timestamp(df, col):
    bad = df.filter(F.col(col).cast("timestamp").isNull()).count()
    return bad == 0, bad
