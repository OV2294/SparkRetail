"""
SparkRetail - Module 4: Real-Time Streaming with Tumbling Windows
==================================================================
Covers:
  - Structured Streaming architecture (micro-batch model)
  - A simulated live order feed (background thread drops CSV files into a
    watched directory -- this is the "streaming source")
  - Streaming DataFrames
  - Tumbling window aggregation on EVENT TIME
  - Watermarking to bound state and handle late-arriving orders
  - Console + memory sinks

Run:
    python module4_streaming.py

The script runs for a bounded period (RUN_SECONDS) and then shuts down
cleanly, so it terminates on its own rather than streaming forever.
"""

import os
import random
import shutil
import sys
import threading
import time
from datetime import datetime, timedelta

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

HERE = os.path.dirname(__file__)
STREAM_DIR = os.path.join(HERE, "..", "output", "stream_input")
CKPT_DIR = os.path.join(HERE, "..", "output", "stream_checkpoint")

# --- Demo timing -----------------------------------------------------
# Deliberately short so the module finishes quickly. The synopsis describes
# 1-minute tumbling windows; 20s is the same concept at a pace you can
# actually watch. Change WINDOW_DURATION to "1 minute" for the report version.
WINDOW_DURATION = "20 seconds"
WATERMARK_DELAY = "10 seconds"
BATCH_INTERVAL_SEC = 2      # how often the simulated feed drops a file
RUN_SECONDS = 70            # total runtime before clean shutdown

REGIONS = ["North", "South", "East", "West", "Central"]
PRODUCTS = [
    ("P001", "Wireless Mouse", 599.0),
    ("P002", "Mechanical Keyboard", 2499.0),
    ("P003", "USB-C Cable", 199.0),
    ("P005", "Bluetooth Speaker", 1799.0),
    ("P010", "Power Bank", 999.0),
]

_stop_feed = threading.Event()


def simulate_feed():
    """Background 'live' order generator.

    Each tick writes ONE csv file into STREAM_DIR. Structured Streaming's file
    source treats each newly-appearing file as a micro-batch of input.
    Files are written to a temp name first, then renamed -- otherwise Spark
    can pick up a half-written file and fail to parse it.
    """
    batch = 0
    while not _stop_feed.is_set():
        batch += 1
        n = random.randint(20, 60)
        lines = ["order_id,order_date,region,product_id,product_name,unit_price,quantity"]
        for i in range(n):
            # Most orders are "now"; ~10% are deliberately late by up to 30s
            # so the watermark has something real to handle.
            ts = datetime.now()
            if random.random() < 0.10:
                ts -= timedelta(seconds=random.randint(5, 30))
            pid, pname, price = random.choice(PRODUCTS)
            lines.append(
                f"ORD-S{batch:04d}-{i:03d},"
                f"{ts.strftime('%Y-%m-%d %H:%M:%S')},"
                f"{random.choice(REGIONS)},{pid},{pname},{price},{random.randint(1, 5)}"
            )
        tmp = os.path.join(STREAM_DIR, f".tmp_batch_{batch}.csv")
        final = os.path.join(STREAM_DIR, f"batch_{batch}.csv")
        with open(tmp, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.rename(tmp, final)
        _stop_feed.wait(BATCH_INTERVAL_SEC)


def main():
    # Fresh directories each run so old batches/checkpoints don't leak in
    for d in (STREAM_DIR, CKPT_DIR):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("SparkRetail-Module4-Streaming")
        .master("local[*, 4]")
        # Small shuffle partition count keeps the console output readable;
        # the default 200 would create a lot of empty partitions per batch.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("region", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    print("=" * 70)
    print("STRUCTURED STREAMING - TUMBLING WINDOW SALES MONITOR")
    print("=" * 70)
    print(f"  source            : file stream at {STREAM_DIR}")
    print(f"  window            : {WINDOW_DURATION} (tumbling / non-overlapping)")
    print(f"  watermark         : {WATERMARK_DELAY} late tolerance")
    print(f"  feed interval     : every {BATCH_INTERVAL_SEC}s")
    print(f"  total runtime     : {RUN_SECONDS}s, then clean shutdown")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Streaming DataFrame
    # ------------------------------------------------------------------
    # maxFilesPerTrigger keeps micro-batches small and predictable.
    stream = (spark.readStream
                   .schema(stream_schema)          # schema is REQUIRED for file streams
                   .option("header", True)
                   .option("maxFilesPerTrigger", 1)
                   .csv(STREAM_DIR))

    print(f"\nIs this a streaming DataFrame? {stream.isStreaming}")

    orders = stream.withColumn(
        "revenue", F.round(F.col("unit_price") * F.col("quantity"), 2)
    )

    # ------------------------------------------------------------------
    # Tumbling window aggregation on event time, with watermark
    # ------------------------------------------------------------------
    windowed = (orders
                .withWatermark("order_date", WATERMARK_DELAY)
                .groupBy(F.window(F.col("order_date"), WINDOW_DURATION),
                         F.col("region"))
                .agg(F.count("*").alias("orders"),
                     F.sum("revenue").cast("decimal(18,2)").alias("revenue"))
                )

    readable = windowed.select(
        F.date_format("window.start", "HH:mm:ss").alias("win_start"),
        F.date_format("window.end", "HH:mm:ss").alias("win_end"),
        "region", "orders", "revenue",
    )

    # ------------------------------------------------------------------
    # Sinks: console (live view) + memory (queryable at the end)
    # ------------------------------------------------------------------
    console_q = (readable.writeStream
                 .outputMode("update")   # only changed windows each batch
                 .format("console")
                 .option("truncate", False)
                 .option("numRows", 20)
                 .trigger(processingTime="5 seconds")
                 .start())

    memory_q = (readable.writeStream
                .outputMode("complete")  # full result table, for the final summary
                .format("memory")
                .queryName("window_totals")
                .option("checkpointLocation", CKPT_DIR)
                .trigger(processingTime="5 seconds")
                .start())

    # Third sink: export a CSV snapshot on every batch, for the Streamlit
    # dashboard to poll. The memory sink above only exists inside THIS
    # script's process -- a separate dashboard process can't query it, so we
    # need something written to disk instead. The snapshot DataFrame is
    # always small (a handful of window/region rows), matching the
    # confirmed-safe collect pattern used elsewhere in this project.
    SNAPSHOT_PATH = os.path.join(HERE, "..", "output", "streaming_snapshot.csv")
    SNAPSHOT_CKPT = os.path.join(HERE, "..", "output", "stream_snapshot_checkpoint")
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    shutil.rmtree(SNAPSHOT_CKPT, ignore_errors=True)

    def export_snapshot(batch_df, batch_id):
        pdf = batch_df.toPandas()
        if not pdf.empty:
            pdf["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pdf["batch_id"] = batch_id
            pdf.to_csv(SNAPSHOT_PATH, index=False)

    snapshot_q = (readable.writeStream
                  .outputMode("complete")
                  .foreachBatch(export_snapshot)
                  .option("checkpointLocation", SNAPSHOT_CKPT)
                  .trigger(processingTime="5 seconds")
                  .start())

    # Start the simulated live feed
    feed = threading.Thread(target=simulate_feed, daemon=True)
    feed.start()
    print("\nSimulated order feed started. Watching windows fill up...\n")

    # ------------------------------------------------------------------
    # Run for a bounded time, then shut down cleanly
    # ------------------------------------------------------------------
    try:
        deadline = time.time() + RUN_SECONDS
        while time.time() < deadline:
            time.sleep(5)
            prog = console_q.lastProgress
            if prog:
                print(f"    [progress] batch={prog.get('batchId')} "
                      f"inputRows={prog.get('numInputRows')} "
                      f"rows/s={prog.get('processedRowsPerSecond', 0):.1f}")
    except KeyboardInterrupt:
        print("\nInterrupted by user -- shutting down.")
    finally:
        _stop_feed.set()
        console_q.stop()
        memory_q.stop()
        snapshot_q.stop()
        feed.join(timeout=5)

    # ------------------------------------------------------------------
    # Final summary from the memory sink
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("FINAL WINDOWED TOTALS (from memory sink)")
    print("=" * 70)
    final = spark.sql("""
        SELECT win_start, win_end, region, orders, revenue
        FROM window_totals
        ORDER BY win_start, revenue DESC
    """)
    final.show(40, truncate=False)

    print("Per-window grand totals across all regions:")
    spark.sql("""
        SELECT win_start, win_end,
               SUM(orders)                          AS total_orders,
               CAST(SUM(revenue) AS DECIMAL(18,2))  AS total_revenue
        FROM window_totals
        GROUP BY win_start, win_end
        ORDER BY win_start
    """).show(truncate=False)

    print("Notes for your report:")
    print("  * TUMBLING windows are fixed-size and non-overlapping -- every order")
    print("    falls into exactly one window, so totals never double-count.")
    print("  * Aggregation is on EVENT TIME (order_date), not arrival time, so a")
    print("    delayed order is still counted in the window it actually belongs to.")
    print(f"  * The {WATERMARK_DELAY} watermark tells Spark how long to keep each")
    print("    window's state in memory. Past that, the window is finalised and")
    print("    anything arriving later is dropped -- this is what stops streaming")
    print("    state from growing without bound.")

    print("\nModule 4 complete.")
    spark.stop()


if __name__ == "__main__":
    main()
