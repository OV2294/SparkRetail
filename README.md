# SparkRetail — real-data edition
**End-to-End Sales Analytics Pipeline with Real-Time Windowed Insights**
Mini Project · Apache Spark & Distributed Data Processing (26PMDS103) · M.Sc. Data Science, Sem I

A PySpark pipeline on a **real retail dataset** — the UCI *Online Retail* transactions of a UK gift retailer
(541,909 invoice lines, 1 Dec 2010 – 9 Dec 2011, 38 countries, 4,372 customers).
It covers RDDs → DataFrames/joins/skew → Spark SQL & Parquet → Structured Streaming with tumbling windows.

**Tested on:** Python 3.14.4 · PySpark 4.2.0 · OpenJDK 25.0.4 (Linux, local mode). No extra JVM flags were needed.

## Dataset
Daqing Chen, Sai Liang Sain, Kun Guo (2012), *Data mining for the online retail industry: a case study of RFM model-based
customer segmentation*, J. Database Marketing & Customer Strategy Management 19(3), 197–208.
Source: <https://archive.ics.uci.edu/dataset/352/online+retail> — licence **CC BY 4.0**. Cite it in your report.

`data/online_retail.csv` (converted from the original Excel file) is included, so the project runs immediately.
`python prepare_data.py --force` rebuilds it from the original `.xlsx` (downloads it if missing; needs `openpyxl`).

Real-data quirks the pipeline deals with (Module 1 prints this profile):

| Issue | Rows | Handling |
|---|---|---|
| Cancellation invoices (`InvoiceNo` starts with `C`) | 9,288 (1.7%) | excluded from sales; analysed separately (cancellation rate, net revenue) |
| Quantity ≤ 0 / unit price ≤ 0 | 10,624 / 2,517 | excluded |
| Missing `CustomerID` (guest checkouts) | 135,080 (24.9%) | kept for revenue, drop out of the customer inner-join |
| Fee/service codes (`POST`, `M`, `BANK CHARGES`, …) | 2,916 | excluded from product analytics |
| **Valid sales lines** | **527,789 (97.4%)** | used for revenue analytics |
| Commas inside quoted descriptions | — | `csv.reader` in the RDD parser (naive `split(",")` would corrupt rows) |

## Layout
```
config.py                    SparkSession factory (pins Python workers to your interpreter), paths
common.py                    schema, CSV parser, business rules (shared by driver & workers)
prepare_data.py              Excel -> CSV, builds the country->region lookup
module1_rdd.py               Spark architecture, lazy eval, RDDs, reduceByKey vs groupByKey, lineage, fault tolerance
module2_dataframe_joins.py   RDD->DataFrame, RFM customer dimension, inner/broadcast joins, skew + key salting
module3_sparksql_storage.py  8 SQL queries, explain(True) / Catalyst, CSV vs Parquet benchmark
module4_streaming.py         replays REAL transactions as a live feed -> tumbling-window aggregation
make_charts.py / run_all.py  PNG charts / run everything
data/  output/               dataset  /  generated CSV tables + PNG charts
```

## Run
```bash
python -m venv .venv
.venv\Scripts\activate 
pip install -r requirements.txt           # JDK 17+ required (JAVA_HOME must point to it; JDK 25 works)
python run_all.py                         # ~5 min; or run modules one by one:
```

manual run
```bash
python module1_rdd.py          [--hold]   # --hold keeps the Spark UI open at http://localhost:4040
python module2_dataframe_joins.py
python module3_sparksql_storage.py [--scale 5]     # --scale N replicates the data N times for the storage benchmark
python module4_streaming.py [--window "1 hour"] [--days 3] [--late-fraction 0.03]
python make_charts.py
```
Standalone cluster: start a master/worker, then `SPARK_MASTER=spark://host:7077 python module1_rdd.py`
(Windows: `set SPARK_MASTER=spark://host:7077`).
**Windows note:** writing CSV/Parquet in Module 3 requires Hadoop's `winutils.exe` + `hadoop.dll` (set `HADOOP_HOME`).
I could only test on Linux.

## What each module shows — and results from my run
**Module 1 — RDDs.** Lazy evaluation (defining the pipeline takes 0.5 s, the first action 9 s, a cached re-count 0.7 s),
narrow vs wide transformations, `reduceByKey` 0.88 s vs `groupByKey` 1.08 s (1.2x, identical output), lineage via
`toDebugString()`, Job/Stage/Task counts, and fault tolerance: partition 2 is made to fail twice, Spark retries from lineage
and still returns 541,909 / 541,909 rows.

**Module 2 — DataFrames & joins.** Derived `line_total` and `discounted_price`, a **customer dimension (4,334 customers, RFM
segments)** joined to sales — the 24.9% guest-checkout lines drop out of the inner join. Broadcast join vs sort-merge
join on a 38-row country→region lookup: 0.31 s vs 0.86 s (2.8x). **Real skew:** the United Kingdom has 91.7% of lines →
one partition holds ~486k of 528k records (max/avg 7.37). Broadcast join and key salting give identical results;
salting brings max/avg down to 1.86.

**Module 3 — Spark SQL & storage.** Queries for revenue by country (UK = 85.2%), top products (gross and *net of
cancellations*), monthly trend with `LAG` growth (Q4 2011 peak), best product per country (`RANK`), weekday demand, monthly
cancellation rate (~12–18%) and top customers. `explain(True)` shows predicate pushdown, constant folding and column pruning.
CSV 63.8 MB → Parquet 4.3 MB (**14.8x smaller**); reads: aggregation 2.7x, filter 3.4x, partition-pruned filter 3.8x faster.

**Module 4 — Streaming.** 3 days of real transactions (10,306 lines, £187,086.65) are replayed in event-time order, one file
per 15 minutes of shop activity. Query A: tumbling 1-hour windows with a 30-minute watermark (update mode, `foreachBatch`).
Query B: same windows × country, no watermark, **memory sink** queried with SQL. A live dashboard prints every ~4 s.
3% of events are delivered 4 hours late: Query B equals the batch truth to the penny; Query A drops 298 of the 303 delayed
lines (£5,240) because their windows were already finalised. With `--late-fraction 0`, Query A equals the batch truth exactly.
Window size is configurable (`--window "1 minute"` is the synopsis example, but this shop only has ~3 lines per minute).

## Honest caveats (good for the viva)
- **Data limits.** The dataset has no separate customer table or region table. The customer dimension is *derived* from the
  transactions (RFM), and `country_region.csv` is a small hand-built lookup of the 38 countries.
- **`discounted_price` is a what-if.** The data contains no discounts; the project simulates a 10% bulk promotion on lines with
  quantity ≥ 24 (see `common.py`).
- **Gross vs net.** The #2 product by gross revenue (`PAPER CRAFT, LITTLE BIRDIE`, 80,995 units in one order) was cancelled
  minutes later; it disappears from the net-of-cancellations ranking (`q2b`). Cancellations are not netted inside `sales`.
- **Streaming is a replay**, not a live source; the file feed can be swapped for Kafka (`readStream.format("kafka")`).
- **Local mode timings.** Everything runs on one machine, so gains are modest and salting even costs extra time here
  (~2.8 s vs 1.3 s for the plain join). The evidence for skew is the partition balance and the Spark UI task-duration
  spread, which matter on a real cluster. Timings vary by machine; the trends hold.
- Adaptive Query Execution is switched off in `config.py` so skew effects are visible; enable it to see Spark's own skew-join handling.
