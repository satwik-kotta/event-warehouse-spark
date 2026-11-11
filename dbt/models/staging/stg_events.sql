select
    e.event_id, e.match_id, e.team_id, e.player_id, e.period, e.minute,
    t.event_type
from {{ source('warehouse', 'fact_event') }} e
join {{ source('warehouse', 'dim_event_type') }} t on e.event_type_id = t.event_type_id
