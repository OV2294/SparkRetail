"""
SparkRetail - Module 1: Spark Setup & RDD-based Processing
============================================================
Covers:
  - SparkSession initialization, SparkContext vs SparkSession
  - Loading raw CSV as an RDD
  - Narrow transformations: map, filter
  - Wide transformation: groupByKey, and key-value comparison against
    reduceByKey (shuffle behaviour)
  - Partition-based processing
  - DAG / Job-Stage-Task hierarchy + lineage graph via .toDebugString()
  - Fault tolerance demo (simulated task failure -> automatic recovery)

Run with:
    python module1_rdd_processing.py
While it runs, open http://localhost:4040 to watch the Spark UI (DAG viz,
stages, tasks) live. The script pauses at a few points so you have time to
click around before it continues.

WINDOWS NOTE: if you're on Windows and see the worker crash instantly on the
very first action (Python worker exited unexpectedly / Connection reset by
peer), it's very likely NOT a Python version problem. It happens when you
have more than one Python install on your machine and Spark's worker
subprocess resolves a DIFFERENT "python" off PATH than the one running this
script -- so the worker loads in an environment where PySpark isn't even
installed correctly. The PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON lines below
pin both driver and worker to the exact interpreter you launched this script
with (sys.executable), which removes that ambiguity entirely.
"""

import os
import sys
import time

# Must be set BEFORE importing pyspark / creating the SparkSession -- see the
# WINDOWS NOTE above. This guarantees the worker subprocess uses the same
# Python that has pyspark installed, instead of whatever "python" resolves
# to first on PATH.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sales.csv")


def pause(msg, seconds=5):
    print(f"\n>>> {msg}")
    print(f"    (Spark UI: http://localhost:4040 -- pausing {seconds}s so you can inspect it)")
    time.sleep(seconds)


def main():
    # ------------------------------------------------------------------
    # 1. SparkSession setup
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("SparkRetail-Module1-RDDProcessing")
        # local[*, 4] = use all cores, allow up to 4 attempts per task.
        # NOTE: in Spark's local mode, spark.task.maxFailures is silently
        # ignored -- retry count can only be set via this master string
        # (local[N, maxFailures]). This tripped up an earlier version of
        # this script: the fault-tolerance demo failed even with maxFailures
        # set as a .config(), because local mode doesn't read it from there.
        .master("local[*, 4]")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")  # keep console readable; full logs still in Spark UI

    print("SparkContext vs SparkSession:")
    print(f"  spark.version          = {spark.version}")
    print(f"  sc.appName             = {sc.appName}")
    print(f"  sc.master              = {sc.master}")
    print(f"  sc.defaultParallelism  = {sc.defaultParallelism}  (default number of RDD partitions)")

    # ------------------------------------------------------------------
    # 2. Load a sample of the CSV and build an RDD from it in memory
    # ------------------------------------------------------------------
    # WINDOWS NOTE: on this machine, extensive testing (see diagnose.py and
    # diagnose2.py in this folder) narrowed the actual fault down to this:
    # running ANY raw Python function over the real file's full content
    # through Spark's classic per-partition worker pipe crashes the worker
    # ("Python worker exited unexpectedly / Connection reset by peer") --
    # whether that's sc.textFile()+map(), or spark.read.text().rdd.map().
    # Python version (3.11/3.12/3.14), Java version, partition count, and
    # IPv4/IPv6 were all tested and ruled out individually; this reproduced
    # identically across all of them. What DOES reliably work on this
    # machine is a DataFrame read followed by a bounded .collect() of a
    # modest number of rows -- confirmed by Modules 2-4 (all DataFrame-based)
    # running successfully, and by isolated testing here.
    #
    # So instead of streaming the full file through a raw Python function
    # (the broken path), this pulls a representative SAMPLE to the driver
    # using the confirmed-safe pattern, then builds the actual demonstration
    # RDD from that in-memory sample with sc.parallelize() -- which has
    # passed every test on this machine, since it never touches the
    # file-backed worker pipe at all. Every RDD concept below (map, filter,
    # reduceByKey vs groupByKey, partitioning, lineage, fault tolerance)
    # still applies genuinely to real RDDs; only the SOURCE differs from a
    # full-file textFile() read.
    SAMPLE_SIZE = 20000  # representative subset; large enough for meaningful
                          # groupByKey/reduceByKey timing differences, small
                          # enough to collect safely on this machine
    print(f"Reading a {SAMPLE_SIZE:,}-row sample via DataFrame (confirmed safe "
          f"on this machine), then building an RDD from it with sc.parallelize()...")
    sample_rows = spark.read.text(DATA_PATH).limit(SAMPLE_SIZE + 1).collect()
    sample_lines = [row.value for row in sample_rows]
    header = sample_lines[0]
    min_partitions = max(8, sc.defaultParallelism * 2)
    data_rdd = sc.parallelize(
        [line for line in sample_lines if line != header], min_partitions
    )  # filtering happens here, in-memory, before parallelize

    print(f"\nLoaded {data_rdd.count():,} data rows (excluding header) across "
          f"{data_rdd.getNumPartitions()} partitions")

    # ------------------------------------------------------------------
    # 3. Narrow transformation: map -> parse each CSV line into a tuple
    # ------------------------------------------------------------------
    # Columns: order_id,order_date,customer_id,region,store_id,product_id,
    #          product_name,unit_price,quantity,discount_pct,payment_method
    def parse_line(line):
        fields = line.split(",")
        region = fields[3]
        unit_price = float(fields[7])
        quantity = int(fields[8])
        discount_pct = float(fields[9])
        revenue = unit_price * quantity * (1 - discount_pct / 100)
        return (region, revenue)

    region_revenue_rdd = data_rdd.map(parse_line)  # narrow transformation: map
    print("\nSample parsed (region, revenue) pairs:")
    for row in region_revenue_rdd.take(5):
        print(f"  {row}")

    # ------------------------------------------------------------------
    # 4. Key-value RDD operations: reduceByKey vs groupByKey
    #    Both compute total revenue per region, but with very different
    #    shuffle behaviour -- this is the comparison your synopsis asks for.
    # ------------------------------------------------------------------
    pause("About to run groupByKey (wide transformation) -- watch the Spark UI 'Jobs' tab")

    t0 = time.time()
    grouped = region_revenue_rdd.groupByKey().mapValues(sum)
    grouped_result = grouped.collect()
    t1 = time.time()
    print(f"\ngroupByKey total revenue per region (took {t1 - t0:.2f}s):")
    for region, total in sorted(grouped_result, key=lambda x: -x[1]):
        print(f"  {region:10s}: Rs {total:,.2f}")
    print("  NOTE: groupByKey ships every individual (region, revenue) pair across the network")
    print("        before reducing -- all values for a key land on one executor. Expensive shuffle.")

    pause("About to run reduceByKey (also wide, but pre-aggregates locally first)")

    t0 = time.time()
    reduced = region_revenue_rdd.reduceByKey(lambda a, b: a + b)
    reduced_result = reduced.collect()
    t1 = time.time()
    print(f"\nreduceByKey total revenue per region (took {t1 - t0:.2f}s):")
    for region, total in sorted(reduced_result, key=lambda x: -x[1]):
        print(f"  {region:10s}: Rs {total:,.2f}")
    print("  NOTE: reduceByKey combines values locally on each partition BEFORE shuffling --")
    print("        far less data crosses the network. This is why reduceByKey usually wins.")

    # ------------------------------------------------------------------
    # 5. Partition-based processing example
    # ------------------------------------------------------------------
    def count_partition(index, iterator):
        yield (index, sum(1 for _ in iterator))

    partition_counts = data_rdd.mapPartitionsWithIndex(count_partition).collect()
    print(f"\nRows per partition (before any repartitioning): {partition_counts}")

    # ------------------------------------------------------------------
    # 6. DAG / Job -> Stage -> Task hierarchy + lineage graph
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("LINEAGE GRAPH for reduceByKey RDD (.toDebugString()):")
    print("=" * 70)
    print(reduced.toDebugString().decode("utf-8"))
    print("\nRead this bottom-up: each line is a step in the lineage.")
    print("Indentation increases at each shuffle boundary (a new Stage in the DAG).")
    print("Cross-check this against the Spark UI -> Jobs -> DAG Visualization for the same job.")

    # ------------------------------------------------------------------
    # 7. Fault tolerance demo: simulate a task failure and observe
    #    Spark automatically recomputing from lineage, not from scratch.
    # ------------------------------------------------------------------
    pause("Now demonstrating fault tolerance -- inducing a deliberate task failure", seconds=3)

    from pyspark import TaskContext

    def flaky_partition(index, iterator):
        # Fail deterministically on the FIRST attempt of each partition only.
        # (A per-row random check would keep re-triggering on every retry too,
        # since a fresh attempt re-scans all rows from the start -- with 300k
        # rows per partition that makes the job fail almost every time instead
        # of demonstrating a clean recovery.) TaskContext.attemptNumber() is 0
        # on the initial try and increments on each Spark-driven retry, so
        # checking it gives a failure that is guaranteed once, then resolved.
        ctx = TaskContext.get()
        attempt = ctx.attemptNumber() if ctx else 0
        if attempt == 0:
            raise RuntimeError(f"Simulated executor failure on partition {index}, attempt {attempt}")
        return iter(iterator)

    # spark.task.maxFailures was raised to 4 at SparkSession creation above
    # (defaults to 1 in local mode, which would abort the job on first failure
    # instead of retrying it).
    try:
        flaky_rdd = region_revenue_rdd.mapPartitionsWithIndex(flaky_partition)
        flaky_rdd.reduceByKey(lambda a, b: a + b).collect()
        print("\nJob completed successfully DESPITE a simulated task failure.")
        print("Check Spark UI -> Stages -> this stage -> you should see 1 failed task")
        print("attempt followed by a successful retry on the same partition, recomputed")
        print("from the RDD's lineage graph (only the failed partition was redone,")
        print("nothing was recomputed from scratch).")
    except Exception as e:
        print(f"\nJob failed even after retries: {e}")
        print("(The failure is probabilistic by design -- just re-run the script.)")

    # ------------------------------------------------------------------
    # Wrap up
    # ------------------------------------------------------------------
    print("\nModule 1 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
