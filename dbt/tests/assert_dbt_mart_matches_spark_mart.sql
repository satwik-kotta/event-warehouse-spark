-- The dbt model must reproduce the Spark SQL mart exactly: same rows, same goals and the same xG.
with d as (select * from {{ ref('fct_team_match') }}),
     s as (select * from {{ source('warehouse', 'mart_team_match') }})
select coalesce(d.match_id, s.match_id) as match_id, coalesce(d.team_id, s.team_id) as team_id
from d
full outer join s on d.match_id = s.match_id and d.team_id = s.team_id
where d.match_id is null or s.match_id is null
   or d.goals_for <> s.goals_for
   or abs(d.xg - s.xg) > 1e-9
   or abs(d.xg_rolling_5 - s.xg_rolling_5) > 1e-9
   or d.shots <> s.shots
