"""How much faster is a question answered from the warehouse than from the raw files?

    python -m warehouse.bench [--raw DIR] [--out DIR]

The question: expected goals and shots per team in one competition-season. Against the raw
JSON, Spark has to parse every event file to find the shots (the files are named by match, not
by season). Against the warehouse it reads one partition of fact_shot. Both use the same engine
on the same machine, and the raw read is given a narrow schema so it is not doing wasted work.
"""
import argparse
import json
import time
from pathlib import Path

from pyspark.sql import functions as F

from . import config, schemas, staging

COMPETITION, SEASON = 11, 27          # La Liga 2015/2016, the largest single season in the source
NARROW = schemas.struct(type=schemas.REF, team=schemas.REF, period=schemas.I,
                        shot=schemas.struct(statsbomb_xg=schemas.D))


def from_raw(spark, raw):
    matches = staging.read_matches(spark, raw).where(
        (F.col("competition.competition_id") == COMPETITION) & (F.col("season.season_id") == SEASON)).select("match_id")
    events = (spark.read.schema(NARROW).option("multiLine", True).json(str(raw / "events" / "*.json"))
              .withColumn("match_id", staging.match_id_from_filename()))
    shots = events.where((F.col("type.name") == "Shot") & (F.col("period") != 5)).join(F.broadcast(matches), "match_id")
    return shots.groupBy(F.col("team.id").alias("team_id")).agg(
        F.count("*").alias("shots"), F.round(F.sum("shot.statsbomb_xg"), 3).alias("xg"))


def from_warehouse(spark, out):
    shots = spark.read.parquet(str(out / "fact_shot")).where(
        f"competition_id = {COMPETITION} AND season_id = {SEASON} AND period <> 5")
    return shots.groupBy("team_id").agg(F.count("*").alias("shots"), F.round(F.sum("xg"), 3).alias("xg"))


def timed(fn, repeats=1):
    best, rows = None, None
    for _ in range(repeats):
        start = time.time()
        rows = sorted(tuple(r) for r in fn().collect())
        best = min(best or 1e9, time.time() - start)
    return best, rows


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--raw", type=Path, default=config.RAW)
    p.add_argument("--out", type=Path, default=config.OUT)
    a = p.parse_args(argv)
    spark = config.quiet(config.spark(app="bench"))
    warehouse_s, warehouse_rows = timed(lambda: from_warehouse(spark, a.out), repeats=3)
    raw_s, raw_rows = timed(lambda: from_raw(spark, a.raw))
    assert raw_rows == warehouse_rows, "the two paths must give the same answer"
    result = {"question": "shots and xG per team, La Liga 2015/2016", "teams": len(raw_rows),
              "raw_json_seconds": round(raw_s, 1), "warehouse_seconds": round(warehouse_s, 2),
              "speedup": round(raw_s / warehouse_s), "answers_identical": True}
    (a.out / "bench.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
