"""Central configuration for SparkRetail (real-data edition: UCI Online Retail).

Tested with: Python 3.14, PySpark 4.2.0, OpenJDK 25.
"""
import os, sys, warnings
warnings.filterwarnings("ignore", category=FutureWarning)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
STREAM_IN  = os.path.join(BASE_DIR, "streaming", "input")

RETAIL_CSV  = os.path.join(DATA_DIR, "online_retail.csv")
COUNTRY_CSV = os.path.join(DATA_DIR, "country_region.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Make Spark's Python workers use the SAME interpreter as the driver (avoids PYTHON_VERSION_MISMATCH
# when several Pythons are installed) and let workers import project modules (common.py).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYTHONPATH"] = BASE_DIR + os.pathsep + os.environ.get("PYTHONPATH", "")

from pyspark.sql import SparkSession  # noqa: E402


def get_spark(app_name="SparkRetail", master="local[4]", shuffle_partitions=8):
    """local[4] = local mode with 4 threads.  SPARK_MASTER=spark://host:7077 -> standalone cluster."""
    spark = (SparkSession.builder
             .appName(app_name)
             .master(os.environ.get("SPARK_MASTER", master))
             .config("spark.sql.shuffle.partitions", shuffle_partitions)
             .config("spark.sql.session.timeZone", "UTC")        # pandas/Spark windows must agree
             .config("spark.sql.adaptive.enabled", "false")      # off so skew effects stay visible
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.addPyFile(os.path.join(BASE_DIR, "common.py"))   # needed on real clusters
    return spark


def banner(title):
    print("\n" + "=" * 78 + "\n" + title + "\n" + "=" * 78)
