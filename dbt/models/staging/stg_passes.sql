select
    event_id, match_id, team_id, player_id, recipient_id,
    is_complete, is_shot_assist, is_goal_assist, is_cross, under_pressure
from {{ source('warehouse', 'fact_pass') }}
