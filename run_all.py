"""Runs the whole SparkRetail pipeline: data -> M1 -> M2 -> M3 -> M4 -> charts  (about 4-5 minutes).
   python run_all.py [--window "1 hour"] [--days 3]"""
import argparse, os, subprocess, sys
ap = argparse.ArgumentParser()
ap.add_argument("--window", default="1 hour"); ap.add_argument("--days", default="3")
a = ap.parse_args()
here = os.path.dirname(os.path.abspath(__file__))
for step in (["prepare_data.py"], ["module1_rdd.py"], ["module2_dataframe_joins.py"], ["module3_sparksql_storage.py"],
             ["module4_streaming.py", "--window", a.window, "--days", a.days], ["make_charts.py"]):
    print(f"\n######## python {' '.join(step)} ########", flush=True)
    if subprocess.run([sys.executable] + step, cwd=here).returncode:
        sys.exit(f"Step failed: {step[0]}")
print("\nDone. Results: ./output  (CSV tables + PNG charts)")
