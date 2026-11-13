# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Benchmark: raw JSON in a volume vs the Delta warehouse
# MAGIC
# MAGIC Same question as `warehouse/bench.py`: shots and expected goals per team, La Liga 2015/2016.
# MAGIC Needs `11:27` in the ingest seasons. The answers from both paths must be identical.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "event_warehouse")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(os.getcwd()).parent))
from warehouse import bench   # noqa: E402

RAW = Path(f"/Volumes/{CATALOG}/{SCHEMA}/raw")
OUT = Path(f"/Volumes/{CATALOG}/{SCHEMA}/build")


def from_delta():
    return spark.sql(f"""
        SELECT team_id, COUNT(*) AS shots, ROUND(SUM(xg), 3) AS xg
        FROM {FQ}.fact_shot
        WHERE competition_id = {bench.COMPETITION} AND season_id = {bench.SEASON} AND period <> 5
        GROUP BY team_id""")


delta_s, delta_rows = bench.timed(from_delta, repeats=3)
raw_s, raw_rows = bench.timed(lambda: bench.from_raw(spark, RAW))
assert raw_rows == delta_rows, "the two paths must give the same answer"
result = {"question": "shots and xG per team, La Liga 2015/2016", "teams": len(raw_rows),
          "raw_json_seconds": round(raw_s, 1), "delta_seconds": round(delta_s, 2),
          "speedup": round(raw_s / delta_s), "answers_identical": True, "compute": "serverless"}
(OUT / "databricks_bench.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
