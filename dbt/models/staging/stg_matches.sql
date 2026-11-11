-- One row per match, with competition and team names attached.
select
    m.match_id,
    m.competition_id,
    m.season_id,
    cs.competition_name,
    cs.season_name,
    m.match_date,
    m.stage,
    m.home_team_id,
    hom.team_name as home_team,
    m.away_team_id,
    awy.team_name as away_team,
    m.home_score,
    m.away_score
from {{ source('warehouse', 'dim_match') }} m
left join {{ source('warehouse', 'dim_competition_season') }} cs
    on m.competition_id = cs.competition_id and m.season_id = cs.season_id
left join {{ source('warehouse', 'dim_team') }} hom on m.home_team_id = hom.team_id
left join {{ source('warehouse', 'dim_team') }} awy on m.away_team_id = awy.team_id
