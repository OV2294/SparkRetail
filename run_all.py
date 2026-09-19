"""
SparkRetail - run the batch modules (1-3) in sequence.

Module 4 (streaming) is intentionally NOT included here: it runs a live feed
for ~70 seconds and is better watched on its own. Run it separately with:
    cd modules && python module4_streaming.py

Usage:
    python run_all.py
"""

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
MODULES_DIR = os.path.join(HERE, "modules")

MODULES = [
    ("Module 1 - Spark Setup & RDD Processing", "module1_rdd_processing.py"),
    ("Module 2 - DataFrame Processing & Joins", "module2_dataframe_joins.py"),
    ("Module 3 - Spark SQL & Storage Optimization", "module3_sql_optimization.py"),
]


def main():
    # Make sure the dataset exists before starting
    sales = os.path.join(HERE, "data", "sales.csv")
    if not os.path.exists(sales):
        print("Dataset not found. Run this first:\n    python generate_data.py")
        sys.exit(1)

    results = []
    for title, script in MODULES:
        print("\n" + "#" * 70)
        print(f"# {title}")
        print("#" * 70 + "\n")
        t0 = time.time()
        # sys.executable keeps every child on the SAME interpreter -- same
        # reasoning as the PYSPARK_PYTHON pin inside each module.
        proc = subprocess.run([sys.executable, script], cwd=MODULES_DIR)
        elapsed = time.time() - t0
        results.append((title, proc.returncode, elapsed))
        if proc.returncode != 0:
            print(f"\n{title} exited with code {proc.returncode}. Stopping.")
            break

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for title, code, elapsed in results:
        status = "OK" if code == 0 else f"FAILED ({code})"
        print(f"  {status:<12s} {elapsed:6.1f}s   {title}")

    print("\nNext: run the streaming module on its own —")
    print("    cd modules && python module4_streaming.py")


if __name__ == "__main__":
    main()
