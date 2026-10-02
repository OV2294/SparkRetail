"""Module 1 - Spark setup & RDD-based processing on the real Online Retail data.

Covers: SparkSession/SparkContext, lazy evaluation, narrow vs wide transformations,
reduceByKey vs groupByKey, lineage (toDebugString), Job-Stage-Task, fault tolerance.
"""
import os, sys, time, tempfile
from pyspark import TaskContext
import config, common

HOLD = "--hold" in sys.argv          # keep Spark UI (http://localhost:4040) open at the end


def best_of(fn, runs=3):
    ts, out = [], None
    for _ in range(runs):
        t = time.time(); out = fn(); ts.append(time.time() - t)
    return out, min(ts)


def main():
    # local[4,4] = 4 threads, a task may fail 4 times before the job fails (needed for the failure demo)
    spark = config.get_spark("SparkRetail-M1-RDD", master="local[4,4]")
    sc = spark.sparkContext

    config.banner("1.1  Spark setup & execution model")
    print("Spark version      :", spark.version)
    print("Master             :", sc.master, "| default parallelism:", sc.defaultParallelism)
    print("Spark UI           :", sc.uiWebUrl)
    print("Driver   = this Python process (SparkContext, DAG scheduler, task scheduler)")
    print("Executors= JVM workers (threads in local mode) running Tasks on partitions")

    # -------------------------------------------------------------- load + lazy evaluation
    config.banner("1.2  Load CSV as RDD - lazy evaluation & narrow transformations")
    t = time.time()
    records = common.load_rdd(sc, config.RETAIL_CSV, 8).cache()                 # (nothing runs yet)
    valid   = records.filter(common.is_valid_sale)                              # narrow
    revenue = valid.map(lambda r: (r[7], r[3] * r[5]))                          # narrow -> (country, revenue)
    print(f"Defined textFile->mapPartitions->filter->map in {time.time()-t:.3f}s (LAZY: no data read yet)")
    t = time.time(); n_raw = records.count(); print(f"First ACTION count() = {n_raw:,} rows in {time.time()-t:.2f}s (reads + parses + caches)")
    t = time.time(); records.count();                  print(f"Second count() from cache in {time.time()-t:.2f}s")
    print("Partitions:", records.getNumPartitions(), "| sample record:", records.first())

    # data-quality profile in ONE pass (map to flags, then reduce)
    flags = records.map(lambda r: (1, r[0].startswith("C"), r[3] <= 0, r[5] <= 0, r[6] is None, r[2] is None,
                                   r[1] in common.NON_PRODUCT)) \
                   .map(lambda f: tuple(int(x) for x in f)).reduce(lambda a, b: tuple(x + y for x, y in zip(a, b)))
    names = ["rows", "cancellation invoices (C...)", "quantity <= 0", "unit price <= 0", "missing CustomerID",
             "missing Description", "non-product codes (POST, M, ...)"]
    print("\nData-quality profile of the REAL data:")
    for k, v in zip(names, flags):
        print(f"  {k:<34}{v:>9,}  ({v / flags[0] * 100:5.2f}%)")
    n_valid = valid.count()
    print(f"  {'VALID sales lines used for revenue':<34}{n_valid:>9,}  ({n_valid / n_raw * 100:5.2f}%)")

    # -------------------------------------------------------------- wide transformations
    config.banner("1.3  Wide transformation: groupByKey vs reduceByKey (shuffle study)")
    revenue.cache().count()
    g, t_g = best_of(lambda: revenue.groupByKey().mapValues(sum).collect())
    r, t_r = best_of(lambda: revenue.reduceByKey(lambda a, b: a + b).collect())
    dg, dr = dict(g), dict(r)
    assert dg.keys() == dr.keys() and all(abs(dg[k] - dr[k]) < 1e-3 * max(1, abs(dr[k])) for k in dg), "results differ"
    print(f"  groupByKey  best of 3: {t_g:6.2f}s\n  reduceByKey best of 3: {t_r:6.2f}s   -> {t_g / t_r:.2f}x faster, identical result")
    print("  reduceByKey combines map-side BEFORE the shuffle; groupByKey ships every record, then sums.\n")
    tot = sum(dr.values())
    print("  Revenue by country - top 8 (GBP):")
    for k, v in sorted(dr.items(), key=lambda x: -x[1])[:8]:
        print(f"    {k:<18}{v:>14,.2f}  {v / tot * 100:5.1f}%")
    print("  (United Kingdom dominates -> real data skew, exploited in Module 2)")

    top_products = (valid.map(lambda r: (r[1], (r[3] * r[5], r[3], r[2])))
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] or b[2]))
                    .takeOrdered(5, key=lambda kv: -kv[1][0]))
    print("\n  Top 5 products by revenue (reduceByKey + takeOrdered):")
    for code, (rev, units, desc) in top_products:
        print(f"    {code:<8}{(desc or '')[:34]:<36}{rev:>12,.2f}  {units:>8,} units")

    # -------------------------------------------------------------- lineage / DAG
    config.banner("1.4  Lineage graph & DAG")
    print(revenue.reduceByKey(lambda a, b: a + b).toDebugString().decode())
    print("\n  Job   = created by each ACTION (count/collect/...).")
    print("  Stage = pipelined narrow transformations; a new stage starts at each SHUFFLE (indent break above).")
    print("  Task  = one stage x one partition (here 8 partitions -> 8 tasks per stage).")
    st = sc.statusTracker()
    for jid in sorted(st.getJobIdsForGroup())[-3:]:
        ji = st.getJobInfo(jid)
        if ji:
            tasks = sum(st.getStageInfo(s).numTasks for s in ji.stageIds if st.getStageInfo(s))
            print(f"  Job {jid}: stages={list(ji.stageIds)} total tasks={tasks} status={ji.status}")

    # -------------------------------------------------------------- fault tolerance
    config.banner("1.5  Fault tolerance: simulated executor/task failure")
    marker_dir = tempfile.mkdtemp(prefix="sparkretail_fail_")
    lines = sc.textFile(config.RETAIL_CSV, 8)

    def flaky(it):
        ctx = TaskContext.get()
        if ctx.partitionId() == 2 and ctx.attemptNumber() < 2:       # partition 2 crashes on attempts 0 and 1
            open(os.path.join(marker_dir, f"fail_p2_attempt{ctx.attemptNumber()}"), "w").close()
            raise RuntimeError("Simulated executor failure")
        yield from common.parse_partition(it)

    expected = n_raw
    sc.setLogLevel("OFF")                                             # hide expected executor stack traces
    got = lines.mapPartitions(flaky).count()
    sc.setLogLevel("ERROR")
    print("  Injected failures:", sorted(os.listdir(marker_dir)))
    print(f"  Expected {expected:,} rows | after recovery {got:,} rows -> "
          f"{'RESULT CORRECT - no data lost' if got == expected else 'MISMATCH'}")
    print("  Spark re-ran ONLY the failed task, rebuilding its partition from the lineage (textFile -> parse).")
    print("  On a standalone cluster you can `kill -9` a worker JVM mid-job and watch the same recovery in the UI.")

    if HOLD:
        input("\nSpark UI is live at " + str(sc.uiWebUrl) + " - press Enter to exit...")
    spark.stop()


if __name__ == "__main__":
    main()
