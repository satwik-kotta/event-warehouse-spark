# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Build the warehouse and publish it to Unity Catalog
# MAGIC
# MAGIC Runs the same `warehouse` package as the local build (staging, star schema, SQL marts, quality checks),
# MAGIC then publishes every table as a managed Delta table with liquid clustering, documented tables and
# MAGIC columns, and primary and foreign key constraints. The comments and keys are what Catalog Explorer,
# MAGIC lineage and Genie read, so the warehouse explains itself to the next person who opens it.

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
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(os.getcwd()).parent))      # repo root, when this runs from a Git folder
from warehouse import marts, model, quality, staging   # noqa: E402

RAW = Path(f"/Volumes/{CATALOG}/{SCHEMA}/raw")
OUT = Path(f"/Volumes/{CATALOG}/{SCHEMA}/build")
RUN_ID, STARTED = uuid.uuid4().hex[:12], datetime.now(timezone.utc)
timings = {}


def step(name, fn):
    s = time.time()
    value = fn()
    timings[name] = round(time.time() - s, 1)
    print(f"{name:<10} {timings[name]:>7.1f}s")
    return value

# COMMAND ----------

stg = step("staging", lambda: staging.stage(spark, RAW, OUT))
tables = step("model", lambda: model.build(spark, stg, OUT))
tables.update(step("marts", lambda: marts.build(spark, tables, OUT)))
checks = step("quality", lambda: quality.run(stg, tables, raw=RAW, out=OUT))

for c in checks:
    print(("PASS" if c["passed"] else ("WARN" if c["severity"] == "warn" else "FAIL")), c["check"], c["failures"], c["detail"])

# COMMAND ----------

# MAGIC %md ## Publish to Unity Catalog

# COMMAND ----------

CLUSTERED = {"fact_event", "fact_pass", "fact_shot", "bridge_shot_player", "fact_lineup", "mart_team_match"}


def publish():
    for name, df in tables.items():
        df.createOrReplaceTempView(f"_src_{name}")
        cluster = "CLUSTER BY (competition_id, season_id)" if name in CLUSTERED else ""
        spark.sql(f"CREATE OR REPLACE TABLE {FQ}.{name} {cluster} AS SELECT * FROM _src_{name}")
    mismatch = spark.read.parquet(str(OUT / "dq_score_mismatch"))
    mismatch.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dq_score_mismatch")


step("publish", publish)

# COMMAND ----------

# Table and column documentation. Genie and Catalog Explorer both read these.
DOCS = {
    "fact_event": ("One row per on-ball event in a match (pass, shot, carry, pressure and so on).", {
        "event_id": "Unique id of the event", "match_second": "Seconds since the start of the period",
        "x": "Pitch x, 0 to 120, attacking left to right", "y": "Pitch y, 0 to 80",
        "under_pressure": "True if an opponent was pressing the player", "outcome": "Outcome of the action when the source records one"}),
    "fact_pass": ("One row per pass. Extends fact_event with pass-only attributes.", {
        "is_complete": "True if the pass reached a teammate", "length": "Pass length in pitch units",
        "is_goal_assist": "True if the pass assisted a goal"}),
    "fact_shot": ("One row per shot, including penalties. Period 5 is a penalty shoot-out and does not count towards the score.", {
        "xg": "Expected goals: the probability the shot becomes a goal, from 0 to 1",
        "is_goal": "True if the shot was scored", "players_in_frame": "Players visible in the freeze frame when the shot was hit"}),
    "bridge_shot_player": ("One row per player visible at the moment of a shot (flattened freeze frame).", {
        "is_teammate": "True if the player is on the shooter's team"}),
    "fact_lineup": ("One row per player per match: who was in the squad and who started.", {
        "is_starter": "True if the player was in the starting eleven", "played": "True if the player took the field"}),
    "dim_match": ("One row per match, with the final score from the match listing.", {
        "home_score": "Goals scored by the home team", "away_score": "Goals scored by the away team", "stage": "Tournament stage, for example Final"}),
    "dim_team": ("One row per team. Names are the most recent the source uses.", {}),
    "dim_player": ("One row per player. known_as is the nickname when there is one.", {}),
    "dim_competition_season": ("One row per competition and season.", {}),
    "dim_date": ("One row per match date.", {}),
    "dim_event_type": ("Lookup of event type names, for example Pass or Shot.", {}),
    "dim_play_pattern": ("Lookup of how a possession started, for example From Corner.", {}),
    "dim_position": ("Lookup of player position names.", {}),
    "mart_team_match": ("One row per team per match: goals, shots, xG, passes and pressures. Start most team questions here.", {
        "result": "W, D or L from this team's point of view", "xg": "Expected goals created, excluding shoot-outs",
        "xg_rolling_5": "Average xG over this team's last five matches in the same competition-season"}),
    "mart_player_season": ("One row per player per competition-season: goals, shots, xG and passes, ranked on xG.", {}),
    "dq_score_mismatch": ("Data quality quarantine: team-matches where goals counted from events differ from the published score.", {}),
}
PKS = {"dim_match": ["match_id"], "dim_team": ["team_id"], "dim_player": ["player_id"], "dim_date": ["date_key"],
       "dim_event_type": ["event_type_id"], "dim_play_pattern": ["play_pattern_id"], "dim_position": ["position_id"],
       "dim_competition_season": ["competition_id", "season_id"], "fact_event": ["event_id"],
       "fact_pass": ["event_id"], "fact_shot": ["event_id"], "fact_lineup": ["match_id", "player_id"]}
FKS = [("fact_event", "match_id", "dim_match"), ("fact_event", "team_id", "dim_team"), ("fact_event", "player_id", "dim_player"),
       ("fact_event", "event_type_id", "dim_event_type"), ("fact_event", "play_pattern_id", "dim_play_pattern"),
       ("fact_event", "position_id", "dim_position"), ("fact_pass", "event_id", "fact_event"),
       ("fact_shot", "event_id", "fact_event"), ("fact_shot", "player_id", "dim_player"),
       ("fact_lineup", "player_id", "dim_player"), ("fact_lineup", "team_id", "dim_team"), ("dim_match", "date_key", "dim_date")]


def q(text):
    return text.replace("'", "''")


def document():
    notes = []
    for t, (comment, cols) in DOCS.items():
        spark.sql(f"COMMENT ON TABLE {FQ}.{t} IS '{q(comment)}'")
        for col, text in cols.items():
            spark.sql(f"ALTER TABLE {FQ}.{t} ALTER COLUMN {col} COMMENT '{q(text)}'")
    for t, keys in PKS.items():
        try:
            for k in keys:
                spark.sql(f"ALTER TABLE {FQ}.{t} ALTER COLUMN {k} SET NOT NULL")
            spark.sql(f"ALTER TABLE {FQ}.{t} ADD CONSTRAINT {t}_pk PRIMARY KEY ({', '.join(keys)})")
        except Exception as e:                    # recorded, not fatal: keys are informational in Unity Catalog
            notes.append(f"pk {t}: {str(e).splitlines()[0][:160]}")
    for child, col, parent in FKS:
        try:
            spark.sql(f"ALTER TABLE {FQ}.{child} ADD CONSTRAINT fk_{child}_{col} FOREIGN KEY ({col}) REFERENCES {FQ}.{parent}")
        except Exception as e:
            notes.append(f"fk {child}.{col}: {str(e).splitlines()[0][:160]}")
    return notes


constraint_notes = step("document", document)
print(f"{len(PKS)} primary keys and {len(FKS)} foreign keys attempted, {len(constraint_notes)} not applied")
for n in constraint_notes:
    print(" ", n)

# COMMAND ----------

# Keep every run's check results, so a dashboard can show data quality over time.
results = spark.createDataFrame([{**c, "run_id": RUN_ID, "run_at": STARTED} for c in checks])
results.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{FQ}.dq_check_results")

summary = {
    "run_id": RUN_ID, "run_at": STARTED.isoformat(), "seconds": timings,
    "tables": {n: spark.table(f"{FQ}.{n}").count() for n in sorted(tables)},
    "checks": {"total": len(checks), "passed": sum(c["passed"] for c in checks),
               "warnings": sum((not c["passed"]) and c["severity"] == "warn" for c in checks),
               "errors": sum((not c["passed"]) and c["severity"] == "error" for c in checks)},
    "constraint_notes": constraint_notes,
}
(OUT / "databricks_run.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
