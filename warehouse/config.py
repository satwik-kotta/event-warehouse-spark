"""Paths and the Spark session. Everything is overridable from the command line or environment."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
RAW = Path(os.environ.get("EWH_RAW", ROOT / "data" / "raw"))
OUT = Path(os.environ.get("EWH_OUT", ROOT / "data" / "warehouse"))

# The open-data commit this warehouse was built from, so a rerun reads the same files.
SOURCE_REPO = "https://github.com/statsbomb/open-data"
SOURCE_COMMIT = "4b73468fc5b0f1950f9f66fada70ad3a4f9327cb"

SHOOTOUT_PERIOD = 5          # penalty shoot-out goals do not count towards the match score


def spark(app="event-warehouse", memory=None, shuffle_partitions=8):
    return (SparkSession.builder.master(os.environ.get("EWH_MASTER", "local[*]")).appName(app)
            .config("spark.driver.memory", memory or os.environ.get("EWH_MEMORY", "5g"))
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.files.maxPartitionBytes", 96 * 1024 * 1024)
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def quiet(session):
    session.sparkContext.setLogLevel("ERROR")
    return session
