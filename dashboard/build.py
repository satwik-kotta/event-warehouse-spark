"""Turn the run report, the benchmark and two marts into docs/data.js for the static dashboard.

    python dashboard/build.py [--out DIR]
"""
import argparse
import json
import sys
from pathlib import Path

import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from warehouse import config  # noqa: E402

GRAIN = {
    "fact_event": "one row per event", "fact_pass": "one row per pass", "fact_shot": "one row per shot",
    "bridge_shot_player": "one row per player in view at a shot", "fact_lineup": "one row per player per match",
    "dim_match": "one row per match", "dim_player": "one row per player", "dim_team": "one row per team",
    "dim_date": "one row per match day", "dim_competition_season": "one row per competition-season",
    "dim_event_type": "one row per event type", "dim_position": "one row per position",
    "dim_play_pattern": "one row per play pattern", "mart_team_match": "one row per team per match",
    "mart_player_season": "one row per player per season",
}


def read(out, name, columns=None):
    return ds.dataset(str(out / name), format="parquet", partitioning="hive").to_table(columns=columns).to_pandas()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=config.OUT)
    a = p.parse_args()
    report = json.loads((a.out / "run_report.json").read_text())
    bench = json.loads((a.out / "bench.json").read_text())

    types = read(a.out, "dim_event_type").set_index("event_type_id").event_type
    counts = read(a.out, "fact_event", ["event_type_id"]).event_type_id.map(types).value_counts()
    top = counts.head(11)
    event_types = [{"name": n, "events": int(v)} for n, v in top.items()]
    event_types.append({"name": f"{len(counts) - len(top)} other types", "events": int(counts.iloc[len(top):].sum())})

    tm = read(a.out, "mart_team_match")
    teams = read(a.out, "dim_team").set_index("team_id").team_name
    seasons = read(a.out, "dim_competition_season")
    seasons["label"] = seasons.competition_name + " " + seasons.season_name
    label = seasons.set_index(["competition_id", "season_id"]).label
    g = tm.groupby(["competition_id", "season_id", "team_id"]).agg(
        matches=("match_id", "count"), goals=("goals_from_shots", "sum"), xg=("xg", "sum")).reset_index()
    g = g[g.matches >= 20]
    team_seasons = [{"team": teams[r.team_id], "season": label[(r.competition_id, r.season_id)], "matches": int(r.matches),
                     "goals": int(r.goals), "xg": round(float(r.xg), 1)} for r in g.itertuples()]

    data = {
        "report": report, "bench": bench, "grain": GRAIN, "event_types": event_types, "team_seasons": team_seasons,
        "team_matches_reconciled": int(len(tm)),
    }
    (ROOT / "docs" / "data.js").write_text("window.DATA = " + json.dumps(data, separators=(",", ":")) + ";\n")
    print(f"wrote docs/data.js: {len(event_types)} event types, {len(team_seasons)} team-seasons")


if __name__ == "__main__":
    main()
