-- One row per team per match: the table most questions start from.
with shots as (
    select match_id, team_id, count(*) as shots, sum(xg) as xg,
           sum(case when is_goal then 1 else 0 end) as goals_from_shots
    from {{ ref('stg_shots') }}
    group by 1, 2
),
passes as (
    select match_id, team_id, count(*) as passes, avg(case when is_complete then 1.0 else 0.0 end) as pass_completion
    from {{ ref('stg_passes') }}
    group by 1, 2
),
activity as (
    select match_id, team_id, count(*) as events,
           sum(case when event_type = 'Pressure' then 1 else 0 end) as pressures,
           sum(case when event_type = 'Own Goal For' then 1 else 0 end) as own_goals_for
    from {{ ref('stg_events') }}
    group by 1, 2
)
select
    s.match_id, s.competition_id, s.season_id, s.match_date, s.team_id, s.opponent_id, s.is_home,
    s.goals_for, s.goals_against,
    case when s.goals_for > s.goals_against then 'W' when s.goals_for < s.goals_against then 'L' else 'D' end as result,
    coalesce(sh.shots, 0) as shots,
    coalesce(sh.xg, 0.0) as xg,
    coalesce(sh.goals_from_shots, 0) as goals_from_shots,
    coalesce(a.own_goals_for, 0) as own_goals_for,
    p.passes, p.pass_completion, a.pressures, a.events,
    avg(coalesce(sh.xg, 0.0)) over (
        partition by s.team_id, s.competition_id, s.season_id
        order by s.match_date, s.match_id
        rows between 4 preceding and current row) as xg_rolling_5
from {{ ref('int_match_sides') }} s
left join shots sh on s.match_id = sh.match_id and s.team_id = sh.team_id
left join passes p on s.match_id = p.match_id and s.team_id = p.team_id
left join activity a on s.match_id = a.match_id and s.team_id = a.team_id
