# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Ingest raw StatsBomb JSON into a Unity Catalog volume
# MAGIC
# MAGIC Creates the schema and two volumes, then downloads the raw files for the chosen competition-seasons
# MAGIC at the same pinned commit the local build uses. The default set (two World Cups, two Euros and
# MAGIC La Liga 2015/2016) is about 600 matches and fits comfortably in Databricks Free Edition.
# MAGIC Set `seasons` to `all` to pull the full 13 GB source.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "event_warehouse")
dbutils.widgets.text("seasons", "43:106,43:3,55:282,55:43,11:27")   # competition_id:season_id, or "all"

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SEASONS = dbutils.widgets.get("seasons").strip()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'StatsBomb open data event warehouse'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw COMMENT 'Raw StatsBomb JSON exactly as downloaded'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.build COMMENT 'Staging Parquet and run reports'")

# COMMAND ----------

import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SOURCE_COMMIT = "4b73468fc5b0f1950f9f66fada70ad3a4f9327cb"     # same as warehouse/config.py
BASE = f"https://raw.githubusercontent.com/statsbomb/open-data/{SOURCE_COMMIT}/data"
RAW = Path(f"/Volumes/{CATALOG}/{SCHEMA}/raw")


def fetch(rel, tries=5):
    dest = RAW / rel
    if dest.exists() and dest.stat().st_size > 0:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    for i in range(tries):
        try:
            with urllib.request.urlopen(f"{BASE}/{rel}", timeout=60) as r:
                body = r.read()
            dest.write_bytes(body)
            return len(body)
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2 ** i)


fetch("competitions.json")
competitions = json.loads((RAW / "competitions.json").read_text())
if SEASONS.lower() == "all":
    wanted = [(c["competition_id"], c["season_id"]) for c in competitions]
else:
    wanted = [tuple(int(v) for v in pair.split(":")) for pair in SEASONS.split(",")]
names = {(c["competition_id"], c["season_id"]): f'{c["competition_name"]} {c["season_name"]}' for c in competitions}

match_ids = []
for cid, sid in wanted:
    fetch(f"matches/{cid}/{sid}.json")
    ms = json.loads((RAW / f"matches/{cid}/{sid}.json").read_text())
    match_ids += [m["match_id"] for m in ms]
    print(f"{names.get((cid, sid), (cid, sid))}: {len(ms)} matches")

files = [f"{kind}/{m}.json" for m in match_ids for kind in ("events", "lineups")]
start = time.time()
with ThreadPoolExecutor(16) as pool:
    downloaded = sum(pool.map(fetch, files))
print(f"{len(files):,} event and lineup files, {downloaded / 1e6:,.0f} MB downloaded this run, {time.time() - start:.0f}s")
