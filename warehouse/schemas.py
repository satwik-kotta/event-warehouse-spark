"""Explicit schemas for the raw JSON.

Letting Spark infer the event schema means reading all 13 GB twice and getting a different
schema whenever a rarely used field is absent from a batch. Declaring it makes the read a
single pass and makes the contract with the source visible in one place.
"""
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

S, I, D, B = StringType(), IntegerType(), DoubleType(), BooleanType()


def struct(**fields):
    return StructType([StructField(k, v, True) for k, v in fields.items()])


REF = struct(id=I, name=S)                      # {"id": 30, "name": "Pass"} appears everywhere
XY = ArrayType(D)                               # [x, y] or [x, y, z]

PASS = struct(recipient=REF, length=D, angle=D, height=REF, end_location=XY, body_part=REF, type=REF,
              outcome=REF, technique=REF, cross=B, switch=B, shot_assist=B, goal_assist=B,
              assisted_shot_id=S)
FREEZE = struct(location=XY, player=REF, position=REF, teammate=B)
SHOT = struct(statsbomb_xg=D, end_location=XY, key_pass_id=S, body_part=REF, outcome=REF, technique=REF,
              type=REF, first_time=B, one_on_one=B, freeze_frame=ArrayType(FREEZE))
OUTCOME_ONLY = struct(outcome=REF)
CARD = struct(card=REF)

EVENT = struct(
    id=S, index=I, period=I, timestamp=S, minute=I, second=I, type=REF, possession=I, possession_team=REF,
    play_pattern=REF, team=REF, player=REF, position=REF, location=XY, duration=D, under_pressure=B,
    counterpress=B, off_camera=B, out=B, related_events=ArrayType(S),
    **{"pass": PASS}, shot=SHOT, carry=struct(end_location=XY), dribble=OUTCOME_ONLY,
    duel=struct(type=REF, outcome=REF), goalkeeper=struct(type=REF, outcome=REF),
    substitution=struct(outcome=REF, replacement=REF), foul_committed=CARD, bad_behaviour=CARD,
    interception=OUTCOME_ONLY, _corrupt_record=S)

# Top-level keys the source is known to send. Anything else is reported as schema drift.
KNOWN_EVENT_KEYS = set(EVENT.fieldNames()) - {"_corrupt_record"} | {
    "tactics", "ball_receipt", "block", "foul_won", "ball_recovery", "50_50", "injury_stoppage",
    "clearance", "miscontrol", "player_off", "half_start", "half_end"}

COUNTRY = struct(id=I, name=S)
MANAGER = struct(id=I, name=S, nickname=S, dob=S, country=COUNTRY)
MATCH = struct(
    match_id=I, match_date=S, kick_off=S,
    competition=struct(competition_id=I, country_name=S, competition_name=S),
    season=struct(season_id=I, season_name=S),
    home_team=struct(home_team_id=I, home_team_name=S, home_team_gender=S, country=COUNTRY,
                     managers=ArrayType(MANAGER)),
    away_team=struct(away_team_id=I, away_team_name=S, away_team_gender=S, country=COUNTRY,
                     managers=ArrayType(MANAGER)),
    home_score=I, away_score=I, match_status=S, match_week=I, last_updated=S,
    competition_stage=REF, stadium=struct(id=I, name=S, country=COUNTRY),
    referee=struct(id=I, name=S, country=COUNTRY))

COMPETITION = struct(competition_id=I, season_id=I, country_name=S, competition_name=S, competition_gender=S,
                     competition_youth=B, competition_international=B, season_name=S)

POSITION_SPELL = struct(position_id=I, position=S, from_period=I, to_period=I, start_reason=S, end_reason=S,
                        **{"from": S, "to": S})
LINEUP = struct(team_id=I, team_name=S, lineup=ArrayType(struct(
    player_id=I, player_name=S, player_nickname=S, jersey_number=I, country=COUNTRY,
    cards=ArrayType(struct(time=S, card_type=S, reason=S, period=I)), positions=ArrayType(POSITION_SPELL))))
