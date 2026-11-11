-- One row per team per competition-season: a report table built on fct_team_match.
-- xg_against comes from the opponent's row of the same match.
with tm as (select * from {{ ref('fct_team_match') }})
select
    tm.competition_id, tm.season_id, tm.team_id, t.team_name,
    count(*) as matches,
    sum(case when tm.result = 'W' then 1 else 0 end) as wins,
    sum(case when tm.result = 'D' then 1 else 0 end) as draws,
    sum(case when tm.result = 'L' then 1 else 0 end) as losses,
    sum(tm.goals_for) as goals_for,
    sum(tm.goals_against) as goals_against,
    round(sum(tm.xg), 2) as xg_for,
    round(sum(opp.xg), 2) as xg_against,
    round(sum(tm.goals_for) - sum(tm.xg), 2) as goals_minus_xg,
    round(avg(tm.pass_completion), 3) as pass_completion
from tm
join tm as opp on tm.match_id = opp.match_id and tm.opponent_id = opp.team_id
join {{ source('warehouse', 'dim_team') }} t on tm.team_id = t.team_id
group by 1, 2, 3, 4
