"""Staging -> star schema.

Grain of each table:
  fact_event            one row per on-ball event                      (key: event_id)
  fact_pass             one row per pass, extends fact_event           (key: event_id)
  fact_shot             one row per shot, extends fact_event           (key: event_id)
  bridge_shot_player    one row per player visible when a shot is hit  (key: event_id + frame_no)
  fact_lineup           one row per player per match                   (key: match_id + player_id)
  dim_*                 one row per id
Facts are partitioned by competition_id / season_id, which is how they are queried.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

PARTS = ["competition_id", "season_id"]


def x(col):
    return F.get(F.col(col), F.lit(0))          # get() returns NULL past the end; [0] raises under ANSI mode


def y(col):
    return F.get(F.col(col), F.lit(1))


def flag(col):
    return F.coalesce(F.col(col), F.lit(False))


# --------------------------------------------------------------------------- dimensions
def dim_competition_season(stg):
    return stg["competitions"].select(
        "competition_id", "season_id", "competition_name", "season_name", "country_name",
        F.col("competition_gender").alias("gender"), F.col("competition_youth").alias("is_youth"),
        F.col("competition_international").alias("is_international")).dropDuplicates(PARTS)


def dim_date(stg):
    d = stg["matches"].select(F.to_date("match_date").alias("date")).distinct()
    return d.select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                    F.year("date").alias("year"), F.month("date").alias("month"),
                    F.dayofweek("date").alias("day_of_week"), F.date_format("date", "EEEE").alias("day_name"))


def dim_team(stg):
    m = stg["matches"]
    home = m.select(F.col("home_team.home_team_id").alias("team_id"), F.col("home_team.home_team_name").alias("team_name"),
                    F.col("home_team.home_team_gender").alias("gender"), F.col("home_team.country.name").alias("country"),
                    "match_date")
    away = m.select(F.col("away_team.away_team_id").alias("team_id"), F.col("away_team.away_team_name").alias("team_name"),
                    F.col("away_team.away_team_gender").alias("gender"), F.col("away_team.country.name").alias("country"),
                    "match_date")
    return latest(home.unionByName(away), "team_id")


def latest(df, key, order="match_date"):
    """Type-1 dimension: keep each id's most recent attributes (names and nicknames change over the years)."""
    w = Window.partitionBy(key).orderBy(F.col(order).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", order)


def dim_player(stg):
    dates = F.broadcast(stg["matches"].select("match_id", "match_date"))
    p = (stg["lineups"].select("match_id", F.explode("lineup").alias("p")).join(dates, "match_id", "left")
         .select(F.col("p.player_id").alias("player_id"), F.col("p.player_name").alias("player_name"),
                 F.coalesce("p.player_nickname", "p.player_name").alias("known_as"),
                 F.col("p.country.name").alias("country"), "match_date"))
    return latest(p, "player_id")


def dim_match(stg):
    m = stg["matches"]
    return m.select(
        "match_id", "competition_id", "season_id",
        F.date_format(F.to_date("match_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.to_date("match_date").alias("match_date"), "kick_off",
        F.col("home_team.home_team_id").alias("home_team_id"), F.col("away_team.away_team_id").alias("away_team_id"),
        "home_score", "away_score", "match_week", F.col("competition_stage.name").alias("stage"),
        F.col("stadium.name").alias("stadium"), F.col("referee.name").alias("referee"))


def dim_lookup(events, col, key, name):
    """Small code tables (event type, play pattern, position) derived from the events themselves."""
    return events.where(F.col(f"{col}.id").isNotNull()).select(
        F.col(f"{col}.id").alias(key), F.col(f"{col}.name").alias(name)).dropDuplicates([key])


# --------------------------------------------------------------------------- facts
def fact_event(events):
    return events.select(
        F.col("id").alias("event_id"), "match_id", *PARTS, F.col("index").alias("event_index"), "period",
        "minute", "second",
        (F.col("minute") * 60 + F.col("second")).alias("match_second"), "possession",
        F.col("type.id").alias("event_type_id"), F.col("play_pattern.id").alias("play_pattern_id"),
        F.col("team.id").alias("team_id"), F.col("possession_team.id").alias("possession_team_id"),
        F.col("player.id").alias("player_id"), F.col("position.id").alias("position_id"),
        x("location").alias("x"), y("location").alias("y"), "duration",
        flag("under_pressure").alias("under_pressure"), flag("counterpress").alias("counterpress"),
        F.coalesce(F.col("pass.outcome.name"), F.col("shot.outcome.name"), F.col("dribble.outcome.name"),
                   F.col("duel.outcome.name"), F.col("goalkeeper.outcome.name"),
                   F.col("interception.outcome.name")).alias("outcome"),
        F.coalesce(F.col("foul_committed.card.name"), F.col("bad_behaviour.card.name")).alias("card"),
        F.size(F.coalesce("related_events", F.array())).alias("related_event_count"))


def fact_pass(events):
    p = events.where(F.col("pass").isNotNull())
    return p.select(
        F.col("id").alias("event_id"), "match_id", *PARTS, F.col("team.id").alias("team_id"),
        F.col("player.id").alias("player_id"), F.col("pass.recipient.id").alias("recipient_id"),
        F.col("pass.length").alias("length"), F.col("pass.angle").alias("angle"),
        F.col("pass.height.name").alias("height"), F.col("pass.type.name").alias("pass_type"),
        F.col("pass.body_part.name").alias("body_part"), x("location").alias("x"), y("location").alias("y"),
        x("pass.end_location").alias("end_x"), y("pass.end_location").alias("end_y"),
        F.col("pass.outcome.name").isNull().alias("is_complete"),          # the source only records failures
        flag("pass.cross").alias("is_cross"), flag("pass.switch").alias("is_switch"),
        flag("pass.shot_assist").alias("is_shot_assist"), flag("pass.goal_assist").alias("is_goal_assist"),
        F.col("pass.assisted_shot_id").alias("assisted_shot_id"), flag("under_pressure").alias("under_pressure"))


def fact_shot(events):
    s = events.where(F.col("shot").isNotNull())
    return s.select(
        F.col("id").alias("event_id"), "match_id", *PARTS, "period", "minute",
        F.col("team.id").alias("team_id"), F.col("player.id").alias("player_id"),
        F.col("shot.statsbomb_xg").alias("xg"), F.col("shot.outcome.name").alias("outcome"),
        (F.col("shot.outcome.name") == "Goal").alias("is_goal"), F.col("shot.type.name").alias("shot_type"),
        F.col("shot.body_part.name").alias("body_part"), F.col("shot.technique.name").alias("technique"),
        x("location").alias("x"), y("location").alias("y"), x("shot.end_location").alias("end_x"),
        y("shot.end_location").alias("end_y"), F.col("shot.key_pass_id").alias("key_pass_id"),
        flag("shot.first_time").alias("is_first_time"), flag("shot.one_on_one").alias("is_one_on_one"),
        flag("under_pressure").alias("under_pressure"),
        F.size(F.coalesce("shot.freeze_frame", F.array())).alias("players_in_frame"))


def bridge_shot_player(events):
    """The nested freeze-frame array, flattened: who was where at the moment of each shot."""
    s = events.where(F.col("shot.freeze_frame").isNotNull())
    f = s.select(F.col("id").alias("event_id"), "match_id", *PARTS, F.posexplode("shot.freeze_frame").alias("frame_no", "f"))
    return f.select("event_id", "match_id", *PARTS, "frame_no", F.col("f.player.id").alias("player_id"),
                    F.col("f.position.id").alias("position_id"), F.col("f.teammate").alias("is_teammate"),
                    x("f.location").alias("x"), y("f.location").alias("y"))


def fact_lineup(stg):
    keys = F.broadcast(stg["matches"].select("match_id", "competition_id", "season_id"))
    l = stg["lineups"].select("match_id", "team_id", F.explode("lineup").alias("p")).join(keys, "match_id", "inner")
    starts = F.filter("p.positions", lambda s: s["start_reason"] == "Starting XI")
    return l.select(
        "match_id", *PARTS, "team_id", F.col("p.player_id").alias("player_id"),
        F.col("p.jersey_number").alias("jersey_number"), (F.size(starts) > 0).alias("is_starter"),
        (F.size("p.positions") > 0).alias("played"), F.get(F.col("p.positions"), F.lit(0))["position_id"].alias("first_position_id"),
        F.size("p.cards").alias("cards"))


def build(spark, stg, out):
    """Write every table and return {name: DataFrame read back from Parquet}."""
    events = stg["events"]
    dims = {
        "dim_competition_season": dim_competition_season(stg), "dim_date": dim_date(stg),
        "dim_team": dim_team(stg), "dim_player": dim_player(stg), "dim_match": dim_match(stg),
        "dim_event_type": dim_lookup(events, "type", "event_type_id", "event_type"),
        "dim_play_pattern": dim_lookup(events, "play_pattern", "play_pattern_id", "play_pattern"),
        "dim_position": dim_lookup(events, "position", "position_id", "position"),
    }
    facts = {"fact_event": fact_event(events), "fact_pass": fact_pass(events), "fact_shot": fact_shot(events),
             "bridge_shot_player": bridge_shot_player(events), "fact_lineup": fact_lineup(stg)}
    for name, df in dims.items():
        df.coalesce(1).write.mode("overwrite").parquet(str(out / name))
    for name, df in facts.items():
        df.write.mode("overwrite").partitionBy(*PARTS).parquet(str(out / name))
    return {name: spark.read.parquet(str(out / name)) for name in list(dims) + list(facts)}
