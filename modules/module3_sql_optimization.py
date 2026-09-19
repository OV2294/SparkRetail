"""
SparkRetail - Module 3: Spark SQL & Storage Optimization
=========================================================
Covers:
  - Registering DataFrames as temporary views
  - Complex aggregation queries in pure Spark SQL
  - Logical vs physical plans via .explain(True) -> Catalyst optimizer
  - Predicate pushdown (observed in the scan node)
  - CSV vs Parquet: write, then benchmark read + filtered-query performance
  - Columnar storage benefits (column pruning)

Run:
    python module3_sql_optimization.py
"""

import os
import shutil
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
OUT = os.path.join(os.path.dirname(__file__), "..", "output")


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def timed(label, fn, repeats=3):
    """Run fn a few times, report the best wall-clock time.

    Best-of-N rather than a single run: one-off JIT warmup and OS file-cache
    effects otherwise dominate and make the comparison meaningless.
    """
    best = float("inf")
    result = None
    for _ in range(repeats):
        t0 = time.time()
        result = fn()
        best = min(best, time.time() - t0)
    print(f"  {label:<45s} {best:.3f}s")
    return best, result


def main():
    spark = (
        SparkSession.builder
        .appName("SparkRetail-Module3-SQLOptimization")
        .master("local[*, 4]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
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

    sales = (spark.read.csv(f"{BASE}/sales.csv", header=True, schema=schema)
                  .withColumn("revenue",
                              F.round(F.col("unit_price") * F.col("quantity") *
                                      (1 - F.col("discount_pct") / 100), 2)))
    customers = spark.read.csv(f"{BASE}/customers.csv", header=True, inferSchema=True)
    regions = spark.read.csv(f"{BASE}/regions.csv", header=True, inferSchema=True)

    # ------------------------------------------------------------------
    # 1. Temporary views
    # ------------------------------------------------------------------
    section("1. TEMPORARY VIEWS")
    sales.createOrReplaceTempView("sales")
    customers.createOrReplaceTempView("customers")
    regions.createOrReplaceTempView("regions")
    print("Registered temp views:")
    spark.sql("SHOW VIEWS").show(truncate=False)

    # ------------------------------------------------------------------
    # 2. Complex SQL queries
    # ------------------------------------------------------------------
    section("2. COMPLEX SQL QUERIES")

    print("Q1 - Regional revenue vs target (join against region lookup):")
    spark.sql("""
        SELECT  s.region,
                r.region_manager,
                CAST(SUM(s.revenue) AS DECIMAL(18,2))          AS total_revenue,
                r.region_target_revenue                        AS target,
                CAST(SUM(s.revenue) * 100.0 / r.region_target_revenue
                     AS DECIMAL(10,2))                         AS pct_of_target,
                COUNT(*)                                       AS orders
        FROM sales s
        JOIN regions r ON s.region = r.region
        GROUP BY s.region, r.region_manager, r.region_target_revenue
        ORDER BY total_revenue DESC
    """).show(truncate=False)

    print("Q2 - Top-selling products per region (window function):")
    spark.sql("""
        WITH product_rev AS (
            SELECT region, product_name,
                   SUM(revenue) AS rev
            FROM sales
            GROUP BY region, product_name
        ), ranked AS (
            SELECT region, product_name, rev,
                   ROW_NUMBER() OVER (PARTITION BY region ORDER BY rev DESC) AS rnk
            FROM product_rev
        )
        SELECT region, product_name,
               CAST(rev AS DECIMAL(18,2)) AS revenue, rnk
        FROM ranked
        WHERE rnk <= 3
        ORDER BY region, rnk
    """).show(20, truncate=False)

    print("Q3 - Monthly revenue trend:")
    spark.sql("""
        SELECT  DATE_FORMAT(order_date, 'yyyy-MM')            AS month,
                CAST(SUM(revenue) AS DECIMAL(18,2))           AS revenue,
                COUNT(*)                                      AS orders,
                CAST(AVG(revenue) AS DECIMAL(18,2))           AS avg_order_value
        FROM sales
        GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
        ORDER BY month
    """).show(truncate=False)

    print("Q4 - Payment method mix by region (pivot):")
    spark.sql("""
        SELECT region, payment_method, COUNT(*) AS orders
        FROM sales
        GROUP BY region, payment_method
        ORDER BY region, orders DESC
    """).show(10, truncate=False)

    # ------------------------------------------------------------------
    # 3. Catalyst optimizer: logical vs physical plan
    # ------------------------------------------------------------------
    section("3. CATALYST OPTIMIZER - LOGICAL VS PHYSICAL PLANS")
    q = spark.sql("""
        SELECT region, CAST(SUM(revenue) AS DECIMAL(18,2)) AS total
        FROM sales
        WHERE region = 'North' AND quantity >= 3
        GROUP BY region
    """)
    print("Full plan tree (.explain(True)) -- parsed -> analyzed -> optimized -> physical:")
    q.explain(True)
    print("\nWhat to look for in your report:")
    print("  * Parsed Logical Plan   : literal translation of the SQL text")
    print("  * Analyzed Logical Plan : column/table names resolved against the catalog")
    print("  * Optimized Logical Plan: Catalyst's rewrites applied (filter pushdown,")
    print("                            constant folding, projection pruning)")
    print("  * Physical Plan         : the actual execution strategy chosen")

    # ------------------------------------------------------------------
    # 4. Predicate pushdown
    # ------------------------------------------------------------------
    section("4. PREDICATE PUSHDOWN")
    print("Scan node for a filtered query over CSV:")
    spark.sql("SELECT order_id, revenue FROM sales WHERE region = 'North'") \
         .explain(mode="formatted")
    print("\nNote 'PushedFilters' in the scan above. With CSV, Spark still reads")
    print("every row off disk -- the filter is applied early but the format cannot")
    print("skip data. Parquet CAN skip whole row groups using its own statistics,")
    print("which is exactly what the benchmark below measures.")

    # ------------------------------------------------------------------
    # 5. CSV vs Parquet
    # ------------------------------------------------------------------
    section("5. STORAGE FORMAT: CSV VS PARQUET")
    os.makedirs(OUT, exist_ok=True)
    csv_path = os.path.join(OUT, "sales_csv")
    parquet_path = os.path.join(OUT, "sales_parquet")
    for p in (csv_path, parquet_path):
        shutil.rmtree(p, ignore_errors=True)

    print("Writing both formats...")
    sales.write.mode("overwrite").option("header", True).csv(csv_path)
    sales.write.mode("overwrite").parquet(parquet_path)

    def dir_size_mb(path):
        total = 0
        for root, _, files in os.walk(path):
            for fn in files:
                total += os.path.getsize(os.path.join(root, fn))
        return total / (1024 * 1024)

    csv_mb = dir_size_mb(csv_path)
    pq_mb = dir_size_mb(parquet_path)
    print(f"\nOn-disk size:")
    print(f"  CSV     : {csv_mb:7.2f} MB")
    print(f"  Parquet : {pq_mb:7.2f} MB   ({csv_mb / pq_mb:.1f}x smaller)")

    print("\nBenchmark A - full count (best of 3):")
    timed("CSV     full count", lambda: spark.read.option("header", True)
          .schema(sales.schema).csv(csv_path).count())
    timed("Parquet full count", lambda: spark.read.parquet(parquet_path).count())
    print("  (Parquet stores row counts in metadata, so this is near-instant.)")

    print("\nBenchmark B - filtered aggregation, 2 of 12 columns (best of 3):")
    csv_t, _ = timed("CSV     filtered aggregation", lambda: spark.read
                     .option("header", True).schema(sales.schema).csv(csv_path)
                     .filter(F.col("region") == "North")
                     .agg(F.sum("revenue")).collect())
    pq_t, _ = timed("Parquet filtered aggregation", lambda: spark.read
                    .parquet(parquet_path)
                    .filter(F.col("region") == "North")
                    .agg(F.sum("revenue")).collect())
    if pq_t > 0:
        print(f"\n  Parquet is {csv_t / pq_t:.1f}x faster on this query.")
    print("  Two effects combine here:")
    print("    1. COLUMN PRUNING    - only `region` and `revenue` are read off disk;")
    print("                           the other 10 columns are never touched.")
    print("    2. PREDICATE PUSHDOWN- row groups whose min/max stats exclude 'North'")
    print("                           are skipped entirely without decompression.")

    print("\nParquet scan node (compare PushedFilters + ReadSchema against the CSV one):")
    (spark.read.parquet(parquet_path)
          .filter(F.col("region") == "North")
          .select("order_id", "revenue")
          .explain(mode="formatted"))

    print("\nModule 3 complete.")
    print(f"Written artifacts: {csv_path}, {parquet_path}")
    spark.stop()


if __name__ == "__main__":
    main()
