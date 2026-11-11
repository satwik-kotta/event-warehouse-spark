-- One row per player per competition-season, ranked on expected goals within that season.
with played as (
    select competition_id, season_id, player_id, count(*) as matches,
           sum(case when is_starter then 1 else 0 end) as starts
    from {{ ref('stg_lineups') }}
    where played
    group by 1, 2, 3
),
shooting as (
    select m.competition_id, m.season_id, s.player_id, count(*) as shots, sum(s.xg) as xg,
           sum(case when s.is_goal then 1 else 0 end) as goals
    from {{ ref('stg_shots') }} s
    join {{ ref('stg_matches') }} m on s.match_id = m.match_id
    group by 1, 2, 3
),
passing as (
    select m.competition_id, m.season_id, p.player_id, count(*) as passes,
           avg(case when p.is_complete then 1.0 else 0.0 end) as pass_completion,
           sum(case when p.is_shot_assist or p.is_goal_assist then 1 else 0 end) as key_passes
    from {{ ref('stg_passes') }} p
    join {{ ref('stg_matches') }} m on p.match_id = m.match_id
    group by 1, 2, 3
)
select
    pl.competition_id, pl.season_id, pl.player_id, pl.matches, pl.starts,
    coalesce(s.shots, 0) as shots, coalesce(s.goals, 0) as goals, coalesce(s.xg, 0.0) as xg,
    coalesce(s.goals, 0) - coalesce(s.xg, 0.0) as goals_minus_xg,
    coalesce(p.passes, 0) as passes, p.pass_completion, coalesce(p.key_passes, 0) as key_passes,
    rank() over (partition by pl.competition_id, pl.season_id order by coalesce(s.xg, 0.0) desc) as xg_rank
from played pl
left join shooting s using (competition_id, season_id, player_id)
left join passing p using (competition_id, season_id, player_id)
