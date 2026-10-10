"""A tiny synthetic source with the same shape as the real one, so tests need no download.

Three matches across two competition-seasons. Scores in the match files agree with the goals
in the event files, including one own goal and one shoot-out penalty that must not be counted.
"""
import json
import uuid

import pytest

from warehouse import config, marts, model, quality, staging


def ref(i, name):
    return {"id": i, "name": name}


PLAYERS = {t: [(t * 100 + n, f"Player {t}-{n}") for n in range(1, 13)] for t in (1, 2, 3)}
TEAMS = {1: "Alpha", 2: "Beta", 3: "Gamma"}


def match_events(home, away, home_goals, away_goals, own_goal_for=None, shootout=False):
    events = []

    def add(type_id, type_name, team, player=None, period=1, minute=0, **extra):
        e = {"id": str(uuid.uuid4()), "index": len(events) + 1, "period": period, "timestamp": "00:00:00.000",
             "minute": minute, "second": 5, "type": ref(type_id, type_name), "possession": 1 + len(events) // 4,
             "possession_team": ref(team, TEAMS[team]), "play_pattern": ref(1, "Regular Play"),
             "team": ref(team, TEAMS[team]), "duration": 0.5}
        if player:
            e.update(player=ref(*player), position=ref(1 + player[0] % 11, f"Position {1 + player[0] % 11}"),
                     location=[60.0, 40.0])
        e.update(extra)
        events.append(e)
        return e

    for team in (home, away):
        add(35, "Starting XI", team, tactics={"formation": 442, "lineup": []})
    for team, goals in ((home, home_goals), (away, away_goals)):
        squad, other = PLAYERS[team], PLAYERS[away if team == home else home]
        for i in range(6):
            add(30, "Pass", team, squad[i], minute=i, under_pressure=i % 2 == 0, **{"pass": {
                "recipient": ref(*squad[i + 1]), "length": 12.5, "angle": 0.3, "height": ref(1, "Ground Pass"),
                "end_location": [70.0, 42.0], "body_part": ref(40, "Right Foot"),
                **({"outcome": ref(9, "Incomplete")} if i == 5 else {}), **({"shot_assist": True} if i == 0 else {})}})
            add(43, "Carry", team, squad[i + 1], minute=i, carry={"end_location": [72.0, 44.0]})
        add(17, "Pressure", team, squad[3], minute=7, counterpress=True)
        shots = goals - (1 if own_goal_for == team else 0)
        for i in range(shots + 2):                                   # two misses plus the goals
            add(16, "Shot", team, squad[9], minute=20 + i, shot={
                "statsbomb_xg": 0.1 + 0.05 * i, "end_location": [120.0, 40.0, 1.2], "body_part": ref(40, "Right Foot"),
                "outcome": ref(97, "Goal") if i < shots else ref(98, "Off T"), "technique": ref(93, "Normal"),
                "type": ref(87, "Open Play"), "first_time": i == 0,
                "freeze_frame": [{"location": [118.0, 40.0], "player": ref(*other[0]), "position": ref(1, "Position 1"),
                                  "teammate": False},
                                 {"location": [100.0, 35.0], "player": ref(*squad[8]), "position": ref(9, "Position 9"),
                                  "teammate": True}]})
        if own_goal_for == team:
            add(25, "Own Goal For", team, minute=50)
            add(20, "Own Goal Against", away if team == home else home, other[2], minute=50)
    add(19, "Substitution", home, PLAYERS[home][10], minute=60,
        substitution={"outcome": ref(103, "Tactical"), "replacement": ref(*PLAYERS[home][11])})
    if shootout:                                                     # scored, but in period 5
        add(16, "Shot", home, PLAYERS[home][9], period=5, minute=120, shot={
            "statsbomb_xg": 0.76, "end_location": [120.0, 38.0, 0.5], "body_part": ref(40, "Right Foot"),
            "outcome": ref(97, "Goal"), "technique": ref(93, "Normal"), "type": ref(88, "Penalty")})
    return events


def lineup(team):
    return {"team_id": team, "team_name": TEAMS[team], "lineup": [
        {"player_id": pid, "player_name": name, "player_nickname": None if n % 2 else f"P{pid}", "jersey_number": n + 1,
         "country": ref(1, "Nowhere"), "cards": [],
         "positions": [] if n == 11 and team != 1 else [
             {"position_id": 1 + pid % 11, "position": f"Position {1 + pid % 11}", "from": "00:00", "to": None,
              "from_period": 1, "to_period": None, "start_reason": "Starting XI" if n < 11 else "Substitution",
              "end_reason": "Final Whistle"}]}
        for n, (pid, name) in enumerate(PLAYERS[team])]}


def match_row(match_id, comp, season, date, home, away, hs, as_):
    side = lambda t, s: {f"{s}_team_id": t, f"{s}_team_name": TEAMS[t], f"{s}_team_gender": "female",
                         "country": ref(1, "Nowhere"), "managers": []}
    return {"match_id": match_id, "match_date": date, "kick_off": "19:00:00.000",
            "competition": {"competition_id": comp, "country_name": "Nowhere", "competition_name": f"Cup {comp}"},
            "season": {"season_id": season, "season_name": f"Season {season}"}, "home_team": side(home, "home"),
            "away_team": side(away, "away"), "home_score": hs, "away_score": as_, "match_status": "available",
            "match_week": 1, "last_updated": "2024-01-01T00:00:00", "competition_stage": ref(1, "Regular Season"),
            "stadium": {"id": 1, "name": "Ground", "country": ref(1, "Nowhere")},
            "referee": {"id": 1, "name": "Ref", "country": ref(1, "Nowhere")}}


FIXTURES = [  # match_id, competition, season, date, home, away, home score, away score, own goal for, shoot-out
    (1001, 10, 1, "2024-03-02", 1, 2, 2, 1, None, False),
    (1002, 10, 1, "2024-03-09", 2, 3, 0, 2, 3, False),
    (2001, 20, 2, "2024-06-15", 3, 1, 1, 1, None, True),
]


def write_source(raw):
    for d in ("events", "lineups"):
        (raw / d).mkdir(parents=True, exist_ok=True)
    seasons = {}
    for mid, comp, season, date, home, away, hs, as_, og, so in FIXTURES:
        (raw / "events" / f"{mid}.json").write_text(json.dumps(match_events(home, away, hs, as_, og, so)))
        (raw / "lineups" / f"{mid}.json").write_text(json.dumps([lineup(home), lineup(away)]))
        seasons.setdefault((comp, season), []).append(match_row(mid, comp, season, date, home, away, hs, as_))
    for (comp, season), rows in seasons.items():
        (raw / "matches" / str(comp)).mkdir(parents=True, exist_ok=True)
        (raw / "matches" / str(comp) / f"{season}.json").write_text(json.dumps(rows))
    (raw / "competitions.json").write_text(json.dumps([
        {"competition_id": c, "season_id": s, "country_name": "Nowhere", "competition_name": f"Cup {c}",
         "competition_gender": "female", "competition_youth": False, "competition_international": False,
         "season_name": f"Season {s}"} for c, s in seasons]))
    return raw


@pytest.fixture(scope="session")
def spark():
    session = config.quiet(config.spark(app="tests", memory="1g", shuffle_partitions=2))
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw(tmp_path_factory):
    return write_source(tmp_path_factory.mktemp("raw"))


@pytest.fixture(scope="session")
def built(spark, raw, tmp_path_factory):
    out = tmp_path_factory.mktemp("warehouse")
    stg = staging.stage(spark, raw, out)
    tables = model.build(spark, stg, out)
    tables.update(marts.build(spark, tables, out))
    return stg, tables, out


@pytest.fixture(scope="session")
def checks(built, raw):
    stg, tables, out = built
    return {c["check"]: c for c in quality.run(stg, tables, raw=raw, out=out)}
