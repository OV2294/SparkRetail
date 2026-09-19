# SparkRetail

**End-to-End Sales Analytics Pipeline with Real-Time Windowed Insights**

Mini Project — Apache Spark & Distributed Data Processing (26PMDS103)
M.Sc. Data Science, Semester I — K.E.S. Shroff College

---

## What this project does

SparkRetail processes retail transaction data through two parallel tracks:

- **Batch track** — raw CSV → RDD transformations → DataFrame joins → Spark SQL with Catalyst optimization → Parquet
- **Streaming track** — simulated live order feed → Structured Streaming → tumbling window aggregations

Both tracks converge into a single analytics pipeline demonstrating Spark's unified engine.

---

## Requirements

| Component | Version |
|---|---|
| Python | 3.10+ (3.11 / 3.12 recommended) |
| PySpark | 4.2.0 |
| Java (JDK) | 17 or later (21 / 25 LTS recommended) |

Install PySpark:

```bash
pip install pyspark==4.2.0
```

Verify Java is visible:

```bash
java -version
```

---

## Project structure

```
SparkRetail/
├── README.md
├── generate_data.py              # run this FIRST
├── run_all.py                    # runs modules 1-3 in sequence
├── data/                         # created by generate_data.py
│   ├── sales.csv                 # 300,000 transactions (fact table)
│   ├── customers.csv             # 2,000 customers (dimension)
│   └── regions.csv               # 5 regions (tiny lookup -> broadcast)
├── modules/
│   ├── module1_rdd_processing.py
│   ├── module2_dataframe_joins.py
│   ├── module3_sql_optimization.py
│   └── module4_streaming.py
└── output/                       # created by modules 3 & 4
    ├── sales_csv/
    ├── sales_parquet/
    ├── stream_input/
    └── stream_checkpoint/
```

---

## How to run

```bash
# 1. Generate the dataset (once)
python generate_data.py

# 2. Run the batch modules
cd modules
python module1_rdd_processing.py
python module2_dataframe_joins.py
python module3_sql_optimization.py

# 3. Run the streaming module (self-terminates after ~70s)
python module4_streaming.py
```

Or run modules 1–3 in one go:

```bash
python run_all.py
```

**While any module runs, open <http://localhost:4040>** to watch the Spark UI —
DAG visualization, stage/task breakdown, and the SQL tab with query plans.
Module 1 pauses at several points specifically so you have time to look.

---

## Module breakdown

### Module 1 — Spark Setup & RDD Processing
- SparkSession vs SparkContext
- Narrow transformations (`map`, `filter`) vs wide (`groupByKey`)
- `reduceByKey` vs `groupByKey` — shuffle behaviour compared with timings
- Partition-based processing via `mapPartitionsWithIndex`
- Lineage graph via `.toDebugString()`
- **Fault tolerance demo** — a task is made to fail on its first attempt, and
  Spark recomputes only that partition from lineage

### Module 2 — DataFrame Processing & Joins
- Explicit schema definition (avoids `inferSchema`'s extra file pass)
- Column expressions → derived `revenue` field
- Inner join (sales × customers) → SortMergeJoin
- **Broadcast join** (sales × regions) → BroadcastHashJoin, with timing comparison
- **Data skew study** — one region holds ~65% of rows
- **Skew mitigation via key salting** with before/after partition distributions
- Two-stage aggregation proving salted totals match unsalted totals

### Module 3 — Spark SQL & Storage Optimization
- Temporary views and complex SQL (window functions, CTEs, joins)
- `.explain(True)` — parsed → analyzed → optimized → physical plans
- Catalyst optimizer behaviour and predicate pushdown
- **CSV vs Parquet benchmark** — on-disk size and query timings

### Module 4 — Structured Streaming with Tumbling Windows
- Simulated live order feed (background thread → file source)
- Streaming DataFrames
- **Tumbling window aggregation on event time**
- **Watermarking** — ~10% of generated orders are deliberately late
- Console sink (live) + memory sink (final summary)
- Bounded run with clean shutdown

---

## Measured results

From a local run on 300,000 transactions (your numbers will differ by machine):

**Storage format comparison (Module 3)**

| Format | On-disk size | Filtered aggregation |
|---|---|---|
| CSV | 31.06 MB | 0.502s |
| Parquet | 5.15 MB | 0.292s |

Parquet was **6.0× smaller** and **1.7× faster** on a query touching 2 of 12 columns.

Two effects combine: **column pruning** (only the needed columns are read) and
**predicate pushdown** (row groups whose min/max statistics exclude the filter
value are skipped without decompression).

**Data skew (Module 2)**

| | Largest partition |
|---|---|
| Before salting | 219,232 rows |
| After salting (12 buckets) | ~44,500 rows |

The dataset is deliberately generated with ~65% of rows in one region so the
skew problem — and the fix — are both visible rather than theoretical.

---

## Troubleshooting

### Windows: "Python worker exited unexpectedly" / "Connection reset by peer"

This crashes on the very first action with no useful Python traceback.

**Cause:** Spark launches its worker as a separate process and resolves
`python` from `PATH`. If you have more than one Python installed, the worker
can get a *different* interpreter than the one running your script — one where
PySpark isn't properly installed.

**Fix (already applied in every module):**

```python
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
```

These must be set *before* importing pyspark. Note this is **not** a Python
version problem — it reproduces identically across Python versions.

If it persists, run `where python` (PowerShell) to see how many installs you
have, and check whether Windows Firewall blocked `python.exe` or `java.exe`
on the loopback socket Spark uses internally.

### Windows: `winutils.exe` not found

A harmless **warning** for this project, since everything reads local files.
To silence it, download `winutils.exe` for your Spark version into
`C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`.

### Task retries don't happen in local mode

`spark.task.maxFailures` is **ignored** in local mode. Retry count must go in
the master string instead: `local[*, 4]` = all cores, up to 4 attempts.
Module 1's fault-tolerance demo depends on this.

### AQE hides the skew demo

Adaptive Query Execution coalesces small shuffle partitions, which on a local
300k-row dataset collapses everything into one partition and makes the
before/after salting comparison identical. Module 2 disables
`spark.sql.adaptive.coalescePartitions.enabled` for that section only.

---

## Alternative datasets

The synthetic generator is self-contained and reproducible (fixed seed), but
these real datasets can be substituted:

- [Online Retail Dataset](https://github.com/databricks/Spark-The-Definitive-Guide/blob/master/data/retail-data/all/online-retail-dataset.csv) — used in *Spark: The Definitive Guide*
- [Retail Analysis on Large Dataset](https://www.kaggle.com/datasets/sahilprajapati143/retail-analysis-large-dataset) — Kaggle
- [Superstore Sales Data](https://www.kaggle.com/datasets/divaadelia/superstore-sales-data) — Kaggle, has region/category breakdowns

---

## References

- Karau, H., Konwinski, A., Wendell, P., Zaharia, M. — *Learning Spark*, O'Reilly
- Chambers, B., Zaharia, M. — *Spark: The Definitive Guide*, O'Reilly
- Karau, H., Warren, R. — *High Performance Spark*, O'Reilly
- Apache Spark Documentation — <https://spark.apache.org/docs/latest/>
