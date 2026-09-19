"""
SparkRetail - Module 2: DataFrame Processing & Joins
=====================================================
Covers:
  - RDD -> DataFrame conversion with an explicit schema
  - Column expressions (deriving discounted price / revenue)
  - Aggregations
  - Inner join   : sales x customers   (both large-ish -> sort-merge join)
  - Broadcast join: sales x regions    (tiny lookup -> broadcast hash join)
  - Data skew study: one region holds ~65% of rows
  - Skew mitigation via key salting, with before/after partition distribution

Run:
    python module2_dataframe_joins.py
"""

import os
import sys
import time

# See WINDOWS NOTE in module1 -- pin driver+worker to the same interpreter.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "data")


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    spark = (
        SparkSession.builder
        .appName("SparkRetail-Module2-DataFrameJoins")
        .master("local[*, 4]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Explicit schema definition (instead of inferSchema)
    # ------------------------------------------------------------------
    section("1. SCHEMA DEFINITION")
    sales_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_date", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("payment_method", StringType(), True),
    ])

    sales = spark.read.csv(f"{BASE}/sales.csv", header=True, schema=sales_schema)
    customers = spark.read.csv(f"{BASE}/customers.csv", header=True, inferSchema=True)
    regions = spark.read.csv(f"{BASE}/regions.csv", header=True, inferSchema=True)

    print("Explicit schema avoids a full extra pass over the file that "
          "inferSchema would need:")
    sales.printSchema()
    print(f"sales rows     = {sales.count():,}")
    print(f"customers rows = {customers.count():,}")
    print(f"regions rows   = {regions.count():,}  <- tiny, ideal broadcast candidate")

    # ------------------------------------------------------------------
    # 2. Column expressions -> derived revenue column
    # ------------------------------------------------------------------
    section("2. COLUMN EXPRESSIONS (derived fields)")
    sales = sales.withColumn(
        "gross_amount", F.col("unit_price") * F.col("quantity")
    ).withColumn(
        "discount_amount", F.col("gross_amount") * (F.col("discount_pct") / 100)
    ).withColumn(
        "revenue", F.round(F.col("gross_amount") - F.col("discount_amount"), 2)
    )
    sales.select("order_id", "region", "unit_price", "quantity",
                 "discount_pct", "revenue").show(5, truncate=False)

    # ------------------------------------------------------------------
    # 3. Aggregations
    # ------------------------------------------------------------------
    section("3. AGGREGATIONS")
    print("Revenue by region:")
    (sales.groupBy("region")
          .agg(F.sum("revenue").cast("decimal(18,2)").alias("total_revenue"),
               F.count("*").alias("orders"),
               F.avg("revenue").cast("decimal(18,2)").alias("avg_order_value"))
          .orderBy(F.desc("total_revenue"))
          .show(truncate=False))

    print("Top 5 products by revenue:")
    (sales.groupBy("product_id", "product_name")
          .agg(F.sum("revenue").cast("decimal(18,2)").alias("total_revenue"))
          .orderBy(F.desc("total_revenue"))
          .limit(5)
          .show(truncate=False))

    # ------------------------------------------------------------------
    # 4. Inner join: sales x customers
    # ------------------------------------------------------------------
    section("4. INNER JOIN (sales x customers)")
    # Disable auto-broadcast so we can genuinely observe a sort-merge join
    # rather than Spark silently broadcasting the customer table.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    joined = sales.join(customers, on="customer_id", how="inner")
    print(f"Rows after inner join: {joined.count():,}")
    joined.select("order_id", "customer_id", "customer_name", "region", "revenue").show(5, truncate=False)

    print("Physical plan (autoBroadcast disabled -> expect SortMergeJoin):")
    joined.explain(mode="formatted")

    # ------------------------------------------------------------------
    # 5. Broadcast join: sales x regions (tiny lookup table)
    # ------------------------------------------------------------------
    section("5. BROADCAST JOIN (sales x regions)")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

    t0 = time.time()
    plain = sales.join(regions.hint("shuffle_hash"), on="region", how="inner")
    plain_count = plain.count()
    t_plain = time.time() - t0

    t0 = time.time()
    broadcasted = sales.join(F.broadcast(regions), on="region", how="inner")
    bcast_count = broadcasted.count()
    t_bcast = time.time() - t0

    print(f"shuffle-hash join : {plain_count:,} rows in {t_plain:.2f}s")
    print(f"broadcast join    : {bcast_count:,} rows in {t_bcast:.2f}s")
    print("\nWhy broadcast wins here: the regions table is 5 rows. Broadcasting")
    print("ships one tiny copy to every executor, so the huge sales table never")
    print("has to be shuffled across the network at all.")

    print("\nPhysical plan for the broadcast join:")
    broadcasted.explain(mode="formatted")

    # ------------------------------------------------------------------
    # 6. Data skew study
    # ------------------------------------------------------------------
    section("6. DATA SKEW STUDY")

    # IMPORTANT: Adaptive Query Execution coalesces small shuffle partitions
    # by default. On a 300k-row local dataset that collapses everything into a
    # SINGLE partition, which completely hides the skew we're trying to show
    # (before AND after salting both read "1 partition, 300000 rows").
    # Turning coalescing off here exposes the real physical distribution.
    # This is a demonstration setting -- in production you'd leave AQE on,
    # since AQE's skew-join handling is itself a mitigation.
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
    spark.conf.set("spark.sql.shuffle.partitions", 12)

    dist = (sales.groupBy("region")
                 .count()
                 .withColumn("pct", F.round(F.col("count") * 100.0 / sales.count(), 2))
                 .orderBy(F.desc("count")))
    dist.show(truncate=False)
    print("One region dominates -- when we group/join on `region`, every row for")
    print("that key is hashed to ONE partition, so one task does most of the work")
    print("while the others sit idle. That straggler task defines job runtime.")

    # Show the imbalance concretely: rows per partition after a repartition by key
    skewed = sales.repartition(F.col("region"))
    per_part = (skewed.withColumn("pid", F.spark_partition_id())
                      .groupBy("pid").count()
                      .filter(F.col("count") > 0)
                      .orderBy(F.desc("count")))
    print("\nRows per non-empty partition when partitioned by region (BEFORE salting):")
    per_part.show(10, truncate=False)

    # ------------------------------------------------------------------
    # 7. Skew mitigation: key salting
    # ------------------------------------------------------------------
    section("7. SKEW MITIGATION VIA KEY SALTING")
    SALT_BUCKETS = 12
    salted = sales.withColumn(
        "salted_region",
        F.concat_ws("_", F.col("region"),
                    (F.rand(seed=7) * SALT_BUCKETS).cast("int"))
    )

    salted_parts = (salted.repartition(F.col("salted_region"))
                          .withColumn("pid", F.spark_partition_id())
                          .groupBy("pid").count()
                          .filter(F.col("count") > 0)
                          .orderBy(F.desc("count")))
    print(f"Rows per non-empty partition after salting into {SALT_BUCKETS} buckets per region (AFTER):")
    salted_parts.show(10, truncate=False)

    print("The hot key is now spread across many partitions, so no single task")
    print("carries the whole load. Two-stage aggregation recovers the true totals:")

    # Stage 1: aggregate per salted key. Stage 2: strip salt, aggregate again.
    two_stage = (salted.groupBy("salted_region")
                       .agg(F.sum("revenue").alias("partial_revenue"))
                       .withColumn("region", F.split(F.col("salted_region"), "_").getItem(0))
                       .groupBy("region")
                       .agg(F.sum("partial_revenue").cast("decimal(18,2)").alias("total_revenue"))
                       .orderBy(F.desc("total_revenue")))
    two_stage.show(truncate=False)

    print("Compare against the plain (unsalted) aggregation -- totals must match:")
    (sales.groupBy("region")
          .agg(F.sum("revenue").cast("decimal(18,2)").alias("total_revenue"))
          .orderBy(F.desc("total_revenue"))
          .show(truncate=False))

    print("\nModule 2 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
