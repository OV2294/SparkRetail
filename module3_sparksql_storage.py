"""Module 3 - Spark SQL, Catalyst optimizer and storage formats (CSV vs Parquet) on real data.
   python module3_sparksql_storage.py [--scale N]   # --scale N replicates the data N times for the storage benchmark
"""
import os, shutil, sys, time
from functools import reduce
import pandas as pd
from pyspark.sql import functions as F
import config, common

SCALE = int(sys.argv[sys.argv.index("--scale") + 1]) if "--scale" in sys.argv else 1
HOLD = "--hold" in sys.argv
CSV_DIR  = os.path.join(config.DATA_DIR, "bench_csv")
PQ_DIR   = os.path.join(config.DATA_DIR, "bench_parquet")
PQP_DIR  = os.path.join(config.DATA_DIR, "bench_parquet_by_month")


def du_mb(path):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(path) for f in fs) / 1048576


def bench(label, build, runs=3):
    ts = []
    for _ in range(runs):
        t = time.time(); build().collect(); ts.append(time.time() - t)
    print(f"  {label:<48} best={min(ts):5.2f}s  avg={sum(ts) / len(ts):5.2f}s")
    return min(ts)


def run(spark, name, title, sql, n=20):
    print(f"\n{name}. {title}")
    df = spark.sql(sql)
    df.show(n, truncate=False)
    pd.DataFrame([r.asDict() for r in df.collect()]).to_csv(os.path.join(config.OUTPUT_DIR, f"{name.lower()}.csv"), index=False)


def main():
    spark = config.get_spark("SparkRetail-M3-SQL")
    sc = spark.sparkContext

    config.banner("3.1  Register temp views")
    tx = common.read_transactions(spark, config.RETAIL_CSV)
    tx.createOrReplaceTempView("transactions")                       # every line (incl. cancellations)
    common.valid_sales(tx).createOrReplaceTempView("sales")          # valid sales lines
    spark.read.csv(config.COUNTRY_CSV, header=True).createOrReplaceTempView("country_region")
    print("Views:", sorted(t.name for t in spark.catalog.listTables()))

    config.banner("3.2  Analytical queries (Spark SQL)")
    run(spark, "q1_country_revenue", "Revenue by country - top 10, with share of total", """
        SELECT s.country, r.region, COUNT(DISTINCT s.invoice_no) AS orders, ROUND(SUM(s.line_total),2) AS revenue,
               ROUND(100*SUM(s.line_total)/SUM(SUM(s.line_total)) OVER (),1) AS pct_of_total
        FROM sales s JOIN country_region r ON s.country = r.country
        GROUP BY s.country, r.region ORDER BY revenue DESC LIMIT 10""")
    run(spark, "q2_top_products", "Top 10 products by GROSS revenue (valid sales lines only)", """
        SELECT stock_code, MAX(description) AS product, SUM(quantity) AS units, ROUND(SUM(line_total),2) AS revenue,
               COUNT(DISTINCT invoice_no) AS orders
        FROM sales GROUP BY stock_code ORDER BY revenue DESC LIMIT 10""")
    run(spark, "q2b_top_products_net", "Top products NET of cancellations (returns/cancels subtract) - note how the gross #2 product vanishes", """
        SELECT stock_code, MAX(description) AS product, SUM(quantity) AS net_units, ROUND(SUM(line_total),2) AS net_revenue
        FROM transactions WHERE is_product AND unit_price > 0 GROUP BY stock_code ORDER BY net_revenue DESC LIMIT 10""")
    run(spark, "q3_monthly_trend", "Monthly revenue with month-over-month growth (window function; Dec-2011 is a partial month)", """
        WITH m AS (SELECT invoice_month, SUM(line_total) AS revenue, COUNT(DISTINCT invoice_no) AS orders,
                          COUNT(DISTINCT customer_id) AS customers FROM sales GROUP BY invoice_month)
        SELECT invoice_month, ROUND(revenue,2) AS revenue, orders, customers,
               ROUND(100*(revenue - LAG(revenue) OVER (ORDER BY invoice_month)) / LAG(revenue) OVER (ORDER BY invoice_month),1) AS mom_pct
        FROM m ORDER BY invoice_month""", 15)
    run(spark, "q4_top_product_per_country", "Best-selling product (by revenue) in each of the top-5 countries - RANK()", """
        WITH top5 AS (SELECT country FROM sales GROUP BY country ORDER BY SUM(line_total) DESC LIMIT 5),
        p AS (SELECT country, MAX(description) AS product, SUM(line_total) AS revenue,
                     RANK() OVER (PARTITION BY country ORDER BY SUM(line_total) DESC) AS rnk
              FROM sales WHERE country IN (SELECT country FROM top5) GROUP BY country, stock_code)
        SELECT country, product, ROUND(revenue,2) AS revenue FROM p WHERE rnk = 1 ORDER BY revenue DESC""")
    run(spark, "q5_weekday_hour", "When do customers buy? Revenue by weekday", """
        SELECT weekday, COUNT(DISTINCT invoice_no) AS orders, ROUND(SUM(line_total),2) AS revenue
        FROM sales GROUP BY weekday ORDER BY revenue DESC""")
    run(spark, "q6_cancellations", "Cancellation rate per month (invoices starting with 'C')", """
        SELECT invoice_month, COUNT(DISTINCT CASE WHEN is_cancel THEN invoice_no END) AS cancelled_invoices,
               COUNT(DISTINCT invoice_no) AS all_invoices,
               ROUND(100*COUNT(DISTINCT CASE WHEN is_cancel THEN invoice_no END)/COUNT(DISTINCT invoice_no),1) AS cancel_pct
        FROM transactions GROUP BY invoice_month ORDER BY invoice_month""", 15)
    run(spark, "q7_top_customers", "Top 10 customers - spend and share of revenue", """
        SELECT customer_id, COUNT(DISTINCT invoice_no) AS orders, ROUND(SUM(line_total),2) AS spend,
               ROUND(100*SUM(line_total)/(SELECT SUM(line_total) FROM sales WHERE customer_id IS NOT NULL),1) AS pct_of_identified_revenue
        FROM sales WHERE customer_id IS NOT NULL GROUP BY customer_id ORDER BY spend DESC LIMIT 10""")

    config.banner("3.3  Catalyst optimizer - explain(True)")
    q = spark.sql("""SELECT country, SUM(rev) AS total FROM
                     (SELECT country, quantity * unit_price AS rev, quantity FROM transactions)
                     WHERE country = 'Germany' AND quantity > 0 AND 1 = 1 GROUP BY country""")
    q.explain(True)
    print("\nWhat to look for in the output:")
    print("  Parsed/Analyzed : the Filter sits ABOVE the subquery/Project, exactly as written.")
    print("  Optimized       : Filter pushed DOWN next to the scan; '1 = 1' folded away; isnotnull() inferred;")
    print("                    unused columns pruned (only country, quantity, unit_price are read).")
    print("  Physical        : FileScan csv with ReadSchema limited to those columns (HashAggregate partial + final).")

    config.banner(f"3.4  Storage formats: CSV vs Parquet (benchmark data = {SCALE}x dataset)")
    base = tx.drop("discounted_price", "discounted_total", "is_product", "weekday", "hour")
    df = reduce(lambda a, b: a.unionByName(b), [base] * SCALE) if SCALE > 1 else base
    for p in (CSV_DIR, PQ_DIR, PQP_DIR): shutil.rmtree(p, ignore_errors=True)
    n = df.count()
    t = time.time(); df.write.option("header", True).csv(CSV_DIR);                        w_csv = time.time() - t
    t = time.time(); df.write.parquet(PQ_DIR);                                            w_pq = time.time() - t
    t = time.time(); df.write.partitionBy("invoice_month").parquet(PQP_DIR);              w_pp = time.time() - t
    s_csv, s_pq, s_pp = du_mb(CSV_DIR), du_mb(PQ_DIR), du_mb(PQP_DIR)
    print(f"  rows={n:,}")
    print(f"  Write  CSV={w_csv:.2f}s  Parquet={w_pq:.2f}s  Parquet(partitioned by month)={w_pp:.2f}s")
    print(f"  Size   CSV={s_csv:.1f} MB  Parquet={s_pq:.1f} MB  Parquet(partitioned)={s_pp:.1f} MB   -> Parquet {s_csv / s_pq:.1f}x smaller")

    from pyspark.sql import types as T
    csv_schema = T.StructType(list(common.SCHEMA.fields) + [          # NB: StructType.add() mutates in place, so build a copy
        T.StructField("is_cancel", T.BooleanType()), T.StructField("line_total", T.DecimalType(18, 2)),
        T.StructField("invoice_month", T.StringType()), T.StructField("invoice_date", T.DateType())])
    rd_csv = lambda: spark.read.csv(CSV_DIR, header=True, schema=csv_schema, timestampFormat="yyyy-MM-dd HH:mm:ss")
    rd_pq, rd_pp = (lambda: spark.read.parquet(PQ_DIR)), (lambda: spark.read.parquet(PQP_DIR))
    print("\n  Benchmark A - aggregation touching 2 of 15 columns (column pruning)")
    a_csv = bench("CSV      SUM(line_total) GROUP BY country", lambda: rd_csv().groupBy("country").agg(F.sum("line_total")))
    a_pq  = bench("Parquet  SUM(line_total) GROUP BY country", lambda: rd_pq().groupBy("country").agg(F.sum("line_total")))
    print("\n  Benchmark B - selective filter (predicate pushdown / partition pruning)")
    b_csv = bench("CSV      WHERE invoice_month='2011-11'", lambda: rd_csv().filter("invoice_month = '2011-11'").agg(F.sum("line_total")))
    b_pq  = bench("Parquet  WHERE invoice_month='2011-11'", lambda: rd_pq().filter("invoice_month = '2011-11'").agg(F.sum("line_total")))
    b_pp  = bench("Parquet partitioned WHERE invoice_month='2011-11'", lambda: rd_pp().filter("invoice_month = '2011-11'").agg(F.sum("line_total")))

    print("\n  Parquet plan (PushedFilters / column pruning):")
    rd_pq().filter("country = 'Germany' AND quantity > 5").select("line_total").explain()
    print("  Partitioned Parquet plan (PartitionFilters = partition pruning):")
    rd_pp().filter("invoice_month = '2011-11'").select("line_total").explain()

    pd.DataFrame([["CSV", s_csv, w_csv, a_csv, b_csv], ["Parquet", s_pq, w_pq, a_pq, b_pq],
                  ["Parquet (partitioned)", s_pp, w_pp, None, b_pp]],
                 columns=["format", "size_mb", "write_s", "agg_read_s", "filter_read_s"]).to_csv(
        os.path.join(config.OUTPUT_DIR, "storage_benchmark.csv"), index=False)
    print(f"\n  Parquet vs CSV speed-up: aggregation {a_csv / a_pq:.1f}x | filter {b_csv / b_pq:.1f}x | partitioned filter {b_csv / b_pp:.1f}x")

    for p in (CSV_DIR, PQ_DIR, PQP_DIR): shutil.rmtree(p, ignore_errors=True)
    if HOLD:
        input("\nSpark UI live at " + str(sc.uiWebUrl) + " - press Enter to exit...")
    spark.stop()


if __name__ == "__main__":
    main()
