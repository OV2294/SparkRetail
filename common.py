"""Shared definitions: schema, CSV parsing (RDD + DataFrame) and business rules. No Spark session here,
so Spark workers can import this module safely."""
import csv
from datetime import datetime
from pyspark.sql import functions as F, types as T

# Lines whose StockCode is a service/fee, not a physical product (postage, manual adjustments, bank charges ...)
NON_PRODUCT = {"POST", "D", "M", "m", "DOT", "BANK CHARGES", "CRUK", "S", "B", "AMAZONFEE", "PADS", "C2"}
SNAPSHOT_DATE = "2011-12-10"          # day after the last transaction; used for RFM recency
BULK_QTY, BULK_DISCOUNT = 24, 0.10    # WHAT-IF promotion used for the derived `discounted_price` column

SCHEMA = T.StructType([
    T.StructField("invoice_no", T.StringType()),    T.StructField("stock_code", T.StringType()),
    T.StructField("description", T.StringType()),   T.StructField("quantity", T.IntegerType()),
    T.StructField("invoice_ts", T.TimestampType()), T.StructField("unit_price", T.DoubleType()),
    T.StructField("customer_id", T.StringType()),   T.StructField("country", T.StringType()),
])


def parse_partition(lines):
    """RDD parser. Uses csv.reader because product descriptions contain quoted commas."""
    for row in csv.reader(lines):
        if not row or row[0] == "InvoiceNo":
            continue
        yield (row[0], row[1], row[2] or None, int(row[3]),
               datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S"), float(row[5]), row[6] or None, row[7])


def load_rdd(sc, path, min_partitions=8):
    return sc.textFile(path, min_partitions).mapPartitions(parse_partition)


def is_valid_sale(r):
    """Plain-python rule used by the RDD module: a real, positive product sale."""
    return (not r[0].startswith("C")) and r[3] > 0 and r[5] > 0 and r[1] not in NON_PRODUCT


def read_transactions(spark, path):
    """All 541,909 lines as a typed DataFrame + derived columns + data-quality flags."""
    df = spark.read.csv(path, header=True, schema=SCHEMA, timestampFormat="yyyy-MM-dd HH:mm:ss")
    return add_derived(df)


def add_derived(df):
    return (df
        .withColumn("is_cancel", F.col("invoice_no").startswith("C"))
        .withColumn("is_product", ~F.col("stock_code").isin(*NON_PRODUCT))
        .withColumn("line_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
        .withColumn("discounted_price",
                    F.round(F.when(F.col("quantity") >= BULK_QTY, F.col("unit_price") * (1 - BULK_DISCOUNT))
                             .otherwise(F.col("unit_price")), 2))
        .withColumn("discounted_total", (F.col("quantity") * F.col("discounted_price")).cast("decimal(18,2)"))
        .withColumn("invoice_month", F.date_format("invoice_ts", "yyyy-MM"))
        .withColumn("invoice_date", F.to_date("invoice_ts"))
        .withColumn("hour", F.hour("invoice_ts"))
        .withColumn("weekday", F.date_format("invoice_ts", "E")))


def valid_sales(df):
    """Sales lines used for revenue analytics (no cancellations, returns, zero-price or fee lines)."""
    return df.filter((~F.col("is_cancel")) & (F.col("quantity") > 0) & (F.col("unit_price") > 0) & F.col("is_product"))
