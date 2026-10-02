"""Module 4 - Real-time streaming with tumbling windows, driven by REAL transactions.

The historical Online Retail transactions of a chosen period are REPLAYED as a live feed: one JSON file per
15 minutes of shop activity is dropped into streaming/input/ every fraction of a second, in event-time order.
Spark Structured Streaming reads the folder as an unbounded table and aggregates sales per tumbling event-time window.

  python module4_streaming.py                                   # 3 days of data, 1-hour windows (~40 s)
  python module4_streaming.py --window "30 minutes" --days 5
  python module4_streaming.py --window "1 minute" --days 1      # the synopsis example (sparse: ~3 orders/minute)
  python module4_streaming.py --late-fraction 0                 # no late data -> stream must equal batch exactly
"""
import argparse, json, os, shutil, threading, time
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T
import config, common

EVENT_SCHEMA = T.StructType([
    T.StructField("invoice_no", T.StringType()), T.StructField("stock_code", T.StringType()),
    T.StructField("country", T.StringType()),    T.StructField("customer_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),  T.StructField("amount", T.DoubleType()),
    T.StructField("invoice_ts", T.StringType()),
])
CKPT = os.path.join(config.BASE_DIR, "streaming", "checkpoint")


def load_slice(start, days):
    df = pd.read_csv(config.RETAIL_CSV, dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str},
                     parse_dates=["InvoiceDate"])
    df = df[(~df.InvoiceNo.str.startswith("C")) & (df.Quantity > 0) & (df.UnitPrice > 0) & (~df.StockCode.isin(common.NON_PRODUCT))]
    s = df[(df.InvoiceDate >= start) & (df.InvoiceDate < start + pd.Timedelta(days=days))].sort_values("InvoiceDate", kind="stable")
    out = pd.DataFrame({"invoice_no": s.InvoiceNo, "stock_code": s.StockCode, "country": s.Country,
                        "customer_id": s.CustomerID, "quantity": s.Quantity, "amount": (s.Quantity * s.UnitPrice).round(2),
                        "invoice_ts": s.InvoiceDate.dt.strftime("%Y-%m-%d %H:%M:%S"), "_t": s.InvoiceDate})
    return out.reset_index(drop=True)


def feed(events, interval, late_fraction, late_files, progress, done):
    """Replays real events in event-time order, one file per 15 min of activity.
    A fraction of events is DELAYED (delivered `late_files` files later) to simulate late-arriving data."""
    rng = np.random.default_rng(7)
    is_late = rng.random(len(events)) < late_fraction
    progress["late_events"] = int(is_late.sum())
    ev = events.assign(_bucket=events["_t"].dt.floor("15min"))
    buckets = sorted(ev["_bucket"].unique())
    progress["total_files"] = len(buckets) + (late_files if late_fraction > 0 else 0)
    pending, n = {}, 0

    def emit(rows):
        nonlocal n
        tmp = os.path.join(config.STREAM_IN, f".tmp_{n}")
        rows.drop(columns=["_t", "_bucket"], errors="ignore").to_json(tmp, orient="records", lines=True)
        os.replace(tmp, os.path.join(config.STREAM_IN, f"orders_{n:05d}.json"))     # atomic publish
        n += 1; progress["files"] = n; time.sleep(interval)

    for i, b in enumerate(buckets):
        mask = (ev["_bucket"] == b).to_numpy()
        on_time, late = ev[mask & ~is_late], ev[mask & is_late]
        if len(late): pending.setdefault(i + late_files, []).append(late)
        rows = pd.concat([on_time] + pending.pop(i, []))
        if len(rows): emit(rows)
    for k in sorted(pending):                              # flush delayed events that are still waiting
        emit(pd.concat(pending[k]))
    done.set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2011-11-21", help="first day of the replay (data covers 2010-12-01..2011-12-09)")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--window", default="1 hour", help="tumbling window size, e.g. '1 hour', '30 minutes', '1 minute'")
    ap.add_argument("--watermark", default="30 minutes")
    ap.add_argument("--interval", type=float, default=0.2, help="seconds between replayed files")
    ap.add_argument("--late-fraction", type=float, default=0.03)
    ap.add_argument("--late-files", type=int, default=16, help="how many files later a delayed event arrives (16 files x 15 min = 4 h)")
    a = ap.parse_args()

    for d in (config.STREAM_IN, CKPT): shutil.rmtree(d, ignore_errors=True)
    os.makedirs(config.STREAM_IN)

    events = load_slice(pd.Timestamp(a.start), a.days)
    print(f"Replaying {len(events):,} REAL transaction lines from {a.start} ({a.days} days)")
    spark = config.get_spark("SparkRetail-M4-Streaming", shuffle_partitions=4)

    config.banner(f"4.1  Streaming DataFrame (file source) | tumbling window = {a.window} | watermark = {a.watermark}")
    stream = (spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 4).json(config.STREAM_IN)
              .withColumn("event_time", F.to_timestamp("invoice_ts")))
    print("isStreaming =", stream.isStreaming)

    def shape(df, extra=()):
        return df.select(F.date_format("window.start", "yyyy-MM-dd HH:mm").alias("window_start"),
                         F.date_format("window.end", "yyyy-MM-dd HH:mm").alias("window_end"), *extra)

    # Query A: tumbling-window totals WITH watermark (late data beyond the watermark is dropped), update mode
    agg_a = shape(stream.withWatermark("event_time", a.watermark).groupBy(F.window("event_time", a.window))
                  .agg(F.round(F.sum("amount"), 2).alias("sales"), F.count("*").alias("lines"),
                       F.size(F.collect_set("invoice_no")).alias("orders"),
                       F.size(F.collect_set("customer_id")).alias("customers")),
                  ["sales", "lines", "orders", "customers"])
    state, lock = {}, threading.Lock()

    def upsert(batch_df, batch_id):
        rows = batch_df.collect()
        with lock:
            for r in rows: state[r.window_start] = r.asDict()

    q_a = (agg_a.writeStream.outputMode("update").foreachBatch(upsert).option("checkpointLocation", CKPT)
           .trigger(processingTime="1 second").start())

    # Query B: totals per window x country, NO watermark, MEMORY sink (queryable with SQL), complete mode
    agg_b = shape(stream.groupBy(F.window("event_time", a.window), "country")
                  .agg(F.round(F.sum("amount"), 2).alias("sales")), ["country", "sales"])
    q_b = (agg_b.writeStream.outputMode("complete").format("memory").queryName("sales_by_country_window")
           .trigger(processingTime="1 second").start())

    config.banner("4.2  Live dashboard (refreshes every ~4 s while the feed replays)")
    progress, done = {"files": 0, "total_files": 0, "late_events": 0}, threading.Event()
    th = threading.Thread(target=feed, args=(events, a.interval, a.late_fraction, a.late_files, progress, done), daemon=True)
    th.start()
    try:
        while not done.is_set():
            time.sleep(4)
            with lock: cur = sorted(state.values(), key=lambda r: r["window_start"])[-5:]
            if not cur: continue
            print(f"\n--- {datetime.now():%H:%M:%S} | files replayed {progress['files']}/{progress['total_files']} | windows so far {len(state)} ---")
            print(pd.DataFrame(cur).to_string(index=False))
            last = cur[-1]["window_start"]
            try:
                top = spark.sql(f"SELECT country, sales FROM sales_by_country_window WHERE window_start='{last}' ORDER BY sales DESC LIMIT 3").collect()
                print("  top countries in latest window:", ", ".join(f"{r.country} {r.sales:,.0f}" for r in top))
            except Exception:
                pass
    except KeyboardInterrupt:
        pass
    done.wait(); q_a.processAllAvailable(); q_b.processAllAvailable()

    # -------------------------------------------------------------- 4.3 reconciliation with the batch truth
    config.banner("4.3  Final windowed totals + reconciliation against a batch computation")
    stream_df = pd.DataFrame(sorted(state.values(), key=lambda r: r["window_start"]))
    wfreq = pd.Timedelta(a.window)
    events["window_start"] = events["_t"].dt.floor(wfreq).dt.strftime("%Y-%m-%d %H:%M")
    truth = events.groupby("window_start").agg(batch_sales=("amount", "sum"), batch_lines=("amount", "size")).reset_index()
    rec = truth.merge(stream_df[["window_start", "window_end", "sales", "lines", "orders", "customers"]], on="window_start", how="left").fillna(0)
    rec["dropped_sales"] = (rec["batch_sales"] - rec["sales"]).round(2)
    rec["dropped_lines"] = (rec["batch_lines"] - rec["lines"]).astype(int)
    rec["batch_sales"] = rec["batch_sales"].round(2)
    show = rec[["window_start", "window_end", "sales", "lines", "orders", "customers", "batch_sales", "dropped_lines"]]
    print(show.head(24).to_string(index=False))
    if len(show) > 24: print(f"... ({len(show)} windows in total; full table in output/streaming_window_totals.csv)")
    rec.to_csv(os.path.join(config.OUTPUT_DIR, "streaming_window_totals.csv"), index=False)

    b_total = spark.sql("SELECT ROUND(SUM(sales),2) AS s FROM sales_by_country_window").first()["s"]
    print(f"\nTotals over the replayed period")
    print(f"  Batch truth (pandas)              : GBP {events['amount'].sum():>12,.2f}  ({len(events):,} lines)")
    print(f"  Query B  (no watermark, memory)   : GBP {b_total:>12,.2f}   <- must equal the batch truth")
    print(f"  Query A  (watermark {a.watermark:<11})    : GBP {rec['sales'].sum():>12,.2f}  ({int(rec['lines'].sum()):,} lines)")
    print(f"  Dropped by the watermark          : GBP {rec['dropped_sales'].sum():>12,.2f}  ({int(rec['dropped_lines'].sum()):,} lines)"
          f"  | late events deliberately delayed: {progress['late_events']:,}")
    print("\nEach window row is NON-overlapping: every order belongs to exactly one window (tumbling).")
    print(f"Delayed events reach Spark ~{a.late_files * 15 / 60:g} h after their event time; once max(event_time) - watermark passes a window's end,")
    print("that window is finalised and later arrivals are dropped (query A), while query B (no watermark) keeps them.")
    print("Re-run with --late-fraction 0 and query A equals the batch truth exactly.")
    q_a.stop(); q_b.stop(); spark.stop()


if __name__ == "__main__":
    main()
