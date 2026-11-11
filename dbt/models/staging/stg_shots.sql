-- Shots that count towards the score: penalty shoot-out kicks (period 5) are removed here, once.
select
    event_id, match_id, team_id, player_id, period, minute,
    xg, is_goal, shot_type, body_part, players_in_frame
from {{ source('warehouse', 'fact_shot') }}
where period <> 5
