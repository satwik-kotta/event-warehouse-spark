import json

from pyspark.sql import functions as F

from tests.conftest import FIXTURES, write_source
from warehouse import marts, model, quality, staging


def failed(stg, tables, **kw):
    return {c["check"] for c in quality.run(stg, tables, **kw) if not c["passed"]}


def test_every_check_passes_on_a_clean_source(checks):
    assert len(checks) == 33
    assert checks["event_files_without_a_listed_match"]["detail"].startswith("0 events held back")
    assert [c for c in checks.values() if not c["passed"]] == []


def test_star_schema_row_counts_and_grain(built):
    _, t, _ = built
    assert t["dim_match"].count() == 3 and t["dim_team"].count() == 3 and t["dim_player"].count() == 36
    assert t["dim_competition_season"].count() == 2 and t["dim_date"].count() == 3
    events = t["fact_event"].count()
    assert events == t["fact_event"].select("event_id").distinct().count()
    assert t["fact_pass"].count() == 6 * 2 * 3                      # six passes per team per match
    assert t["fact_shot"].count() == 7 + 5 + 6 + 1                  # goals plus two misses per team, plus the shoot-out
    assert t["bridge_shot_player"].count() == 18 * 2                # the shoot-out penalty has no freeze frame
    assert t["fact_lineup"].count() == 12 * 2 * 3


def test_nested_fields_are_flattened_correctly(built):
    _, t, _ = built
    p = t["fact_pass"].where("match_id = 1001 AND team_id = 1")
    assert p.where("is_complete").count() == 5 and p.where("is_shot_assist").count() == 1
    assert {r.end_x for r in p.select("end_x").collect()} == {70.0}
    f = t["bridge_shot_player"].where("frame_no = 0").select("x", "y", "is_teammate").first()
    assert (f.x, f.y, f.is_teammate) == (118.0, 40.0, False)
    assert t["dim_player"].where("player_id = 101").first().known_as == "P101"       # nickname preferred
    assert t["dim_player"].where("player_id = 102").first().known_as == "Player 1-2"  # falls back to the name
    bench = t["fact_lineup"].where("match_id = 1001 AND team_id = 2 AND player_id = 212").first()
    assert not bench.played and not bench.is_starter and bench.first_position_id is None


def test_facts_are_partitioned_by_competition_and_season(built):
    _, _, out = built
    parts = sorted(p.parent.name + "/" + p.name for p in (out / "fact_event").glob("competition_id=*/season_id=*"))
    assert parts == ["competition_id=10/season_id=1", "competition_id=20/season_id=2"]


def test_team_match_mart_agrees_with_the_scoreboard(built):
    _, t, _ = built
    tm = {(r.match_id, r.team_id): r for r in t["mart_team_match"].collect()}
    assert len(tm) == 6
    for mid, _, _, _, home, away, hs, as_, og, _ in FIXTURES:
        h, a = tm[(mid, home)], tm[(mid, away)]
        assert (h.goals_for, a.goals_for) == (hs, as_)
        assert h.goals_from_shots + h.own_goals_for == hs and a.goals_from_shots + a.own_goals_for == as_
        assert h.is_home and not a.is_home and h.opponent_id == away
    assert tm[(1002, 3)].own_goals_for == 1 and tm[(1002, 3)].result == "W"
    assert tm[(2001, 3)].result == "D" and tm[(2001, 3)].shots == 3            # shoot-out penalty left out
    assert abs(tm[(1001, 1)].pass_completion - 5 / 6) < 1e-9
    assert abs(tm[(1002, 2)].xg_rolling_5 - (tm[(1001, 2)].xg + tm[(1002, 2)].xg) / 2) < 1e-9


def test_player_season_mart_ranks_within_a_season(built):
    _, t, _ = built
    ps = t["mart_player_season"]
    top = ps.where("competition_id = 10 AND season_id = 1 AND player_id = 110").first()
    assert top.goals == 2 and top.shots == 4 and top.matches == 1 and top.xg_rank <= 2
    assert abs(top.goals_minus_xg - (2 - 0.7)) < 1e-9
    assert ps.where("competition_id = 10 AND xg_rank <= 2").count() == 2         # only the two strikers have any xG
    assert ps.where("competition_id = 20 AND player_id = 310").first().goals == 1  # shoot-out goal not counted
    assert ps.where("player_id = 212").count() == 0                              # unused substitute


def test_checks_catch_broken_data(built, raw):
    stg, t, _ = built
    ev = t["fact_event"]
    one = ev.where("match_id = 1001").orderBy("event_index").limit(1)

    assert "event_index_is_gap_free" in failed(stg, {**t, "fact_event": ev.where("NOT (match_id = 1001 AND event_index = 9)")})
    assert "fact_event_key_unique" in failed(stg, {**t, "fact_event": ev.unionByName(one)})
    assert "fk_fact_event.player_id" in failed(stg, {**t, "dim_player": t["dim_player"].where("player_id <> 110")})
    assert "every_match_has_events" in failed(stg, {**t, "fact_event": ev.where("match_id <> 2001")})
    assert "fact_shot_matches_shot_events" in failed(stg, {**t, "fact_shot": t["fact_shot"].limit(5)})
    assert "xg_is_a_probability" in failed(stg, {**t, "fact_shot": t["fact_shot"].withColumn("xg", F.col("xg") + 1)})
    assert "dim_team_key_unique" in failed(stg, {**t, "dim_team": t["dim_team"].unionByName(t["dim_team"].limit(1))})


def test_a_wrong_score_is_warned_about_and_quarantined(built, spark, tmp_path):
    stg, t, _ = built
    wrong = t["mart_team_match"].withColumn(
        "goals_for", F.when((F.col("match_id") == 1001) & (F.col("team_id") == 1), 5).otherwise(F.col("goals_for")))
    result = {c["check"]: c for c in quality.run(stg, {**t, "mart_team_match": wrong}, out=tmp_path)}
    check = result["event_goals_equal_match_score"]
    assert check["severity"] == "warn" and check["failures"] == 1
    row = spark.read.parquet(str(tmp_path / "dq_score_mismatch")).first()
    assert (row.match_id, row.team_id, row.goals_for, row.goals_in_events) == (1001, 1, 5, 2)


def test_truncated_file_and_new_field_are_detected(spark, tmp_path):
    raw = write_source(tmp_path / "raw")
    f = raw / "events" / "1001.json"
    events = json.loads(f.read_text())
    events[3]["brand_new_field"] = {"a": 1}
    f.write_text(json.dumps(events))
    unknown, sampled = quality.schema_drift(raw)
    assert unknown == {"brand_new_field": 1} and sampled == 3

    f.write_text(f.read_text()[:2000])                              # cut off mid-record
    corrupt = staging.read_events(spark, raw).cache()
    assert corrupt.where("match_id = 1001 AND _corrupt_record IS NOT NULL").count() == 1
    assert corrupt.where("match_id = 1002 AND _corrupt_record IS NULL").count() > 30


def test_event_file_without_a_listed_match_is_held_back(spark, tmp_path):
    raw = write_source(tmp_path / "raw")
    orphan = json.loads((raw / "events" / "1001.json").read_text())
    for e in orphan:
        e["id"] = "orphan-" + e["id"]
    (raw / "events" / "9999.json").write_text(json.dumps(orphan))
    stg = staging.stage(spark, raw, tmp_path / "out")
    assert stg["events_unmatched"].select("match_id").distinct().collect()[0].match_id == 9999
    assert stg["events_unmatched"].count() == len(orphan)
    assert stg["events"].where("match_id = 9999").count() == 0
    tables = model.build(spark, stg, tmp_path / "out")
    assert tables["fact_event"].where("match_id = 9999").count() == 0
    tables.update(marts.build(spark, tables, tmp_path / "out"))
    results = {c["check"]: c for c in quality.run(stg, tables)}
    held = results["event_files_without_a_listed_match"]
    assert held["severity"] == "warn" and held["failures"] == 1
    assert [c for c in results.values() if not c["passed"] and c["severity"] == "error"] == []
