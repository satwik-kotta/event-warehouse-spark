"""Checks on the staged data, the star schema and the marts.

`error` checks stop the run: they mean the warehouse itself is wrong (a lost file, a duplicated key,
a fact pointing at a dimension row that is not there). `warn` checks are recorded and the offending
rows are written to a quarantine table: they mean the source disagrees with itself, which a reader
of the data needs to know about but a rerun cannot fix.
"""
import json
import random

from pyspark.sql import functions as F

from . import schemas


def result(name, severity, failures, detail):
    failures = int(failures)
    return {"check": name, "severity": severity, "passed": failures == 0, "failures": failures, "detail": detail}


def orphans(child, parent, key, parent_key=None):
    """Rows of `child` whose non-null `key` has no match in `parent`."""
    p = parent.select(F.col(parent_key or key).alias(key)).distinct()
    return child.where(F.col(key).isNotNull()).join(p, key, "left_anti").count()


def duplicates(df, *keys):
    return df.count() - df.select(*keys).distinct().count()


def schema_drift(raw, sample=150, seed=7):
    """Top-level event fields the declared schema does not know about, from a sample of raw files."""
    files = sorted((raw / "events").glob("*.json"))
    random.Random(seed).shuffle(files)
    unknown = {}
    for f in files[:sample]:
        for event in json.loads(f.read_text()):
            for k in event.keys() - schemas.KNOWN_EVENT_KEYS:
                unknown[k] = unknown.get(k, 0) + 1
    return unknown, min(sample, len(files))


def run(stg, t, raw=None, out=None):
    """`stg` is the staging dict, `t` the star schema plus marts. Returns a list of check results."""
    ev, r = t["fact_event"], []

    # ---- nothing lost or mangled on the way in ------------------------------------------------
    corrupt = stg["events"].where(F.col("_corrupt_record").isNotNull()).count()
    r.append(result("raw_event_files_parse", "error", corrupt, "files Spark could not parse as JSON"))

    listed = t["dim_match"].select("match_id")
    with_events = ev.select("match_id").distinct()
    r.append(result("every_match_has_events", "error", listed.join(with_events, "match_id", "left_anti").count(),
                    f"{listed.count():,} matches listed"))
    lost = stg["events_unmatched"].agg(F.countDistinct("match_id"), F.count("*")).first()
    r.append(result("event_files_without_a_listed_match", "warn", lost[0],
                    f"{lost[1]:,} events held back in staging; {with_events.count():,} files loaded"))

    per_match = ev.groupBy("match_id").agg(F.count("*").alias("n"), F.max("event_index").alias("hi"),
                                           F.min("event_index").alias("lo"),
                                           F.countDistinct("event_index").alias("d"))
    broken = per_match.where("n <> hi OR lo <> 1 OR d <> n").count()
    r.append(result("event_index_is_gap_free", "error", broken,
                    "within each match the source numbers events 1..n; a gap means truncated input"))

    # ---- keys ----------------------------------------------------------------------------------
    r.append(result("fact_event_key_unique", "error", duplicates(ev, "event_id"), f"{ev.count():,} events"))
    for name, key in [("dim_match", "match_id"), ("dim_team", "team_id"), ("dim_player", "player_id"),
                      ("dim_event_type", "event_type_id"), ("dim_position", "position_id"),
                      ("dim_play_pattern", "play_pattern_id"), ("dim_date", "date_key")]:
        r.append(result(f"{name}_key_unique", "error", duplicates(t[name], key), f"{t[name].count():,} rows"))
    r.append(result("dim_competition_season_key_unique", "error",
                    duplicates(t["dim_competition_season"], "competition_id", "season_id"),
                    f"{t['dim_competition_season'].count():,} rows"))
    r.append(result("fact_lineup_key_unique", "error", duplicates(t["fact_lineup"], "match_id", "player_id"),
                    f"{t['fact_lineup'].count():,} rows"))

    # ---- every foreign key resolves ------------------------------------------------------------
    fks = [("fact_event", "team_id", "dim_team", None), ("fact_event", "player_id", "dim_player", None),
           ("fact_event", "event_type_id", "dim_event_type", None), ("fact_event", "position_id", "dim_position", None),
           ("fact_event", "play_pattern_id", "dim_play_pattern", None),
           ("fact_pass", "recipient_id", "dim_player", "player_id"), ("fact_shot", "player_id", "dim_player", None),
           ("fact_lineup", "team_id", "dim_team", None), ("bridge_shot_player", "player_id", "dim_player", None),
           ("dim_match", "date_key", "dim_date", None), ("dim_match", "home_team_id", "dim_team", "team_id")]
    for child, key, parent, pkey in fks:
        r.append(result(f"fk_{child}.{key}", "error", orphans(t[child], t[parent], key, pkey), f"-> {parent}"))
    seasons = t["dim_competition_season"].select("competition_id", "season_id")
    r.append(result("fk_dim_match.competition_season", "error",
                    t["dim_match"].join(seasons, ["competition_id", "season_id"], "left_anti").count(),
                    "-> dim_competition_season"))

    # ---- the detail tables add up to the event table -------------------------------------------
    types = t["dim_event_type"]
    by_type = dict(ev.join(F.broadcast(types), "event_type_id").groupBy("event_type").count().collect())
    passes, shots = t["fact_pass"].count(), t["fact_shot"].count()
    r.append(result("fact_pass_matches_pass_events", "error", abs(passes - by_type.get("Pass", 0)), f"{passes:,} passes"))
    r.append(result("fact_shot_matches_shot_events", "error", abs(shots - by_type.get("Shot", 0)), f"{shots:,} shots"))
    frames = t["fact_shot"].agg(F.sum("players_in_frame")).first()[0] or 0
    bridge = t["bridge_shot_player"].count()
    r.append(result("bridge_matches_freeze_frames", "error", abs(bridge - frames), f"{bridge:,} player positions"))

    # ---- values --------------------------------------------------------------------------------
    bad_xg = t["fact_shot"].where("xg IS NULL OR xg < 0 OR xg > 1").count()
    r.append(result("xg_is_a_probability", "error", bad_xg, "0 <= xg <= 1 on every shot"))
    off_pitch = ev.where("x < 0 OR x > 120.5 OR y < 0 OR y > 80.5").count()
    r.append(result("locations_on_the_pitch", "warn", off_pitch, "pitch is 120 x 80 in source units"))

    # ---- two independent sources agree ---------------------------------------------------------
    # The match file states the final score. The event files contain every goal. They are recorded
    # separately, so counting goals from events and comparing is a real end-to-end check.
    tm = t["mart_team_match"]
    mismatch = tm.where("goals_from_shots + own_goals_for <> goals_for").select(
        "match_id", "competition_id", "season_id", "team_id", "goals_for",
        (F.col("goals_from_shots") + F.col("own_goals_for")).alias("goals_in_events"))
    n = mismatch.count()
    if out is not None:
        mismatch.coalesce(1).write.mode("overwrite").parquet(str(out / "dq_score_mismatch"))
    r.append(result("event_goals_equal_match_score", "warn", n,
                    f"{tm.count():,} team-matches compared; disagreements are in dq_score_mismatch"))

    if raw is not None:
        unknown, sampled = schema_drift(raw)
        r.append(result("no_undeclared_event_fields", "warn", len(unknown),
                        f"{sampled} raw files sampled" + (f"; new fields {sorted(unknown)}" if unknown else "")))
    return r
