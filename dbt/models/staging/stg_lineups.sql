select l.match_id, m.competition_id, m.season_id, l.team_id, l.player_id, l.is_starter, l.played
from {{ source('warehouse', 'fact_lineup') }} l
join {{ source('warehouse', 'dim_match') }} m on l.match_id = m.match_id
