"""Module 2 - DataFrame processing & joins on the real data.

RDD -> DataFrame, derived columns, RFM customer profile (inner join), broadcast join with a country lookup,
and data-skew study: the REAL data is skewed (United Kingdom = ~91% of rows) -> broadcast join + key salting.
"""
import os, sys, time
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
import config, common

HOLD = "--hold" in sys.argv


def timed(label, df, runs=3):
    ts = []
    for _ in range(runs):
        t = time.time(); n = df.count(); ts.append(time.time() - t)
    print(f"  [{label:<24}] rows={n:>9,}  best of {runs}: {min(ts):5.2f}s")
    return min(ts)


def partition_sizes(df, col, n=8):
    return df.repartition(n, col).rdd.glom().map(len).collect()


def ratio(sizes):
    return max(sizes) / (sum(sizes) / len(sizes))


def main():
    spark = config.get_spark("SparkRetail-M2-DataFrames")
    sc = spark.sparkContext

    # -------------------------------------------------------------- 2.1 RDD -> DataFrame
    config.banner("2.1  RDD -> schema-based DataFrame, cleaning & derived columns")
    rdd = common.load_rdd(sc, config.RETAIL_CSV)
    raw = common.add_derived(spark.createDataFrame(rdd, common.SCHEMA))
    sales = common.valid_sales(raw).cache()
    n_raw, n_sales = raw.count(), sales.count()
    print(f"Raw lines={n_raw:,}   valid sales lines={n_sales:,}  (removed {n_raw - n_sales:,}: cancellations, returns, fees, zero prices)")
    sales.printSchema()
    sales.select("invoice_no", "description", "quantity", "unit_price", "discounted_price", "line_total",
                 "discounted_total", "invoice_month").show(6, truncate=30)
    print(f"`discounted_price` = WHAT-IF bulk promotion: {int(common.BULK_DISCOUNT*100)}% off lines with quantity >= {common.BULK_QTY}.")
    w = sales.agg(F.sum("line_total").alias("list"), F.sum("discounted_total").alias("promo")).first()
    print(f"Total revenue GBP {w['list']:,.2f}  ->  with bulk promo GBP {w['promo']:,.2f}  (promo cost GBP {w['list'] - w['promo']:,.2f})")

    # -------------------------------------------------------------- 2.2 customer dim (RFM) + inner join
    config.banner("2.2  Customer dimension (RFM segments) + inner join")
    snap = F.lit(common.SNAPSHOT_DATE).cast("date")
    cust = (sales.filter(F.col("customer_id").isNotNull()).groupBy("customer_id")
            .agg(F.countDistinct("invoice_no").alias("orders"),
                 F.round(F.sum("line_total").cast("double"), 2).alias("spend"),
                 F.datediff(snap, F.max("invoice_date")).alias("recency_days")))
    from pyspark.sql.window import Window
    r = F.ntile(4).over(Window.orderBy(F.desc("recency_days")))     # 4 = most recent
    f = F.ntile(4).over(Window.orderBy("orders"))
    m = F.ntile(4).over(Window.orderBy("spend"))
    cust = (cust.withColumn("r", r).withColumn("f", f).withColumn("m", m)
            .withColumn("rfm", F.col("r") + F.col("f") + F.col("m"))
            .withColumn("segment", F.when(F.col("rfm") >= 11, "Champions").when(F.col("rfm") >= 9, "Loyal")
                        .when(F.col("rfm") >= 6, "Potential").when(F.col("rfm") >= 4, "At Risk").otherwise("Hibernating"))
            .cache())
    print(f"Customer dimension built from the transactions: {cust.count():,} customers")
    cust.groupBy("segment").agg(F.count("*").alias("customers"), F.round(F.avg("spend"), 2).alias("avg_spend"),
                                F.round(F.avg("recency_days"), 1).alias("avg_recency_days")) \
        .orderBy(F.desc("avg_spend")).show()

    joined = sales.join(cust, "customer_id", "inner")
    print(f"Inner join sales x customers: {joined.count():,} of {n_sales:,} lines kept")
    print(f"  -> {n_sales - joined.count():,} lines ({(n_sales - joined.count()) / n_sales * 100:.1f}%) have no CustomerID (guest checkout) and drop out of an INNER join.")
    print("Revenue by RFM segment:")
    seg = (joined.groupBy("segment").agg(F.round(F.sum("line_total").cast("double"), 2).alias("revenue"),
                                         F.countDistinct("customer_id").alias("customers")).orderBy(F.desc("revenue")))
    seg.show()
    pd.DataFrame([r.asDict() for r in seg.collect()]).to_csv(os.path.join(config.OUTPUT_DIR, "rfm_segments.csv"), index=False)

    # -------------------------------------------------------------- 2.3 broadcast join
    config.banner("2.3  Broadcast join: sales x country->region lookup (38 rows)")
    regions = spark.read.csv(config.COUNTRY_CSV, header=True)
    sales.count()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)       # force a shuffle (sort-merge) join
    t_smj = timed("sort-merge join", sales.join(regions, "country"))
    t_bhj = timed("broadcast hash join", sales.join(F.broadcast(regions), "country"))
    print(f"  Broadcast speed-up: {t_smj / t_bhj:.2f}x   (lookup is copied to every executor; the big side is not shuffled)")
    print("\n  Physical plan with broadcast hint (look for BroadcastHashJoin / BroadcastExchange):")
    plan = sales.join(F.broadcast(regions), "country")._jdf.queryExecution().executedPlan().toString()
    print("\n".join("   " + l[:150] for l in plan.splitlines() if any(k in l for k in ("Join", "Broadcast", "Scan"))))
    print("\nRevenue by region (via broadcast join):")
    (sales.join(F.broadcast(regions), "country").groupBy("region")
          .agg(F.round(F.sum("line_total").cast("double"), 2).alias("revenue"), F.count("*").alias("lines"))
          .orderBy(F.desc("revenue")).show())

    # -------------------------------------------------------------- 2.4 skew
    config.banner("2.4  Data skew in the REAL data: United Kingdom dominates")
    dist = sales.groupBy("country").count().orderBy(F.desc("count"))
    dist.show(5)
    top = dist.first()
    print(f"  '{top['country']}' holds {top['count'] / n_sales * 100:.1f}% of all sales lines.")
    before = partition_sizes(sales, "country")
    print("  Records per partition after hash-partitioning on country:", before)
    print(f"  max/avg partition size = {ratio(before):.2f}  (ideal = 1.0; one task gets almost everything)")

    agg = lambda d: (d.groupBy("country", "region").agg(F.round(F.sum("line_total").cast("double"), 2).alias("revenue"),
                                                         F.count("*").alias("lines")))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    print("\n  (a) Baseline - shuffle (sort-merge) join on the skewed key")
    t0 = time.time(); base = {r.country: (r.revenue, r.lines) for r in agg(sales.join(regions, "country")).collect()}
    t_a = time.time() - t0; print(f"      {t_a:.2f}s")

    print("  (b) Mitigation 1 - broadcast join (no shuffle of the big side)")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    t0 = time.time(); bc = {r.country: (r.revenue, r.lines) for r in agg(sales.join(F.broadcast(regions), "country")).collect()}
    t_b = time.time() - t0; print(f"      {t_b:.2f}s")

    print("  (c) Mitigation 2 - key salting (only the hot key is split into N sub-keys; use when the dim is too big to broadcast)")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    N, HOT = 8, top["country"]
    fact_s = sales.withColumn("salt", F.when(F.col("country") == HOT, (F.rand(42) * N).cast("int")).otherwise(F.lit(0)))
    salts = spark.range(0, N).select(F.col("id").cast("int").alias("salt"))
    dim_s = (regions.filter(F.col("country") != HOT).withColumn("salt", F.lit(0))
             .unionByName(regions.filter(F.col("country") == HOT).crossJoin(salts)))      # hot key replicated N times
    t0 = time.time(); sl = {r.country: (r.revenue, r.lines) for r in agg(fact_s.join(dim_s, ["country", "salt"])).collect()}
    t_c = time.time() - t0; print(f"      {t_c:.2f}s")

    same = all(base[k][1] == bc[k][1] == sl[k][1] and abs(base[k][0] - bc[k][0]) < 0.05 and abs(base[k][0] - sl[k][0]) < 0.05 for k in base)
    print(f"\n  Results identical across (a), (b), (c)? -> {same}")
    after = (fact_s.withColumn("key", F.concat_ws("_", "country", F.col("salt").cast("string")))
             .repartition(8, "key").rdd.glom().map(len).collect())
    print("  Partition sizes before salting:", before)
    print("  Partition sizes after  salting:", after)
    print(f"  max/avg ratio: {ratio(before):.2f}  ->  {ratio(after):.2f}")
    pd.DataFrame({"partition": range(len(before)), "before_salting": before, "after_salting": after}).to_csv(
        os.path.join(config.OUTPUT_DIR, "skew_partitions.csv"), index=False)
    print("  Note: in local mode every 'executor' shares the same CPU, so wall-clock gains are modest; the partition-balance")
    print("  ratio and the Spark UI > Stages > task-duration spread are the real evidence of skew on a cluster.")

    if HOLD:
        input("\nSpark UI live at " + str(sc.uiWebUrl) + " - press Enter to exit...")
    spark.stop()


if __name__ == "__main__":
    main()
