-- One row per team per match: the table most questions start from.
-- Shoot-out penalties (period 5) are excluded because they do not count towards the score.
WITH shots AS (
    SELECT match_id, team_id,
           COUNT(*)                                   AS shots,
           SUM(xg)                                    AS xg,
           SUM(CASE WHEN is_goal THEN 1 ELSE 0 END)   AS goals_from_shots
    FROM fact_shot
    WHERE period <> 5
    GROUP BY match_id, team_id
),
passes AS (
    SELECT match_id, team_id,
           COUNT(*)                                             AS passes,
           AVG(CAST(CAST(is_complete AS INT) AS DOUBLE))     AS pass_completion
    FROM fact_pass
    GROUP BY match_id, team_id
),
activity AS (
    SELECT e.match_id, e.team_id,
           COUNT(*)                                                         AS events,
           SUM(CASE WHEN t.event_type = 'Pressure' THEN 1 ELSE 0 END)       AS pressures,
           SUM(CASE WHEN t.event_type = 'Own Goal For' THEN 1 ELSE 0 END)   AS own_goals_for
    FROM fact_event e
    JOIN dim_event_type t ON e.event_type_id = t.event_type_id
    GROUP BY e.match_id, e.team_id
),
sides AS (
    SELECT match_id, competition_id, season_id, match_date, home_team_id AS team_id, away_team_id AS opponent_id,
           TRUE AS is_home, home_score AS goals_for, away_score AS goals_against
    FROM dim_match
    UNION ALL
    SELECT match_id, competition_id, season_id, match_date, away_team_id, home_team_id,
           FALSE, away_score, home_score
    FROM dim_match
)
SELECT s.match_id, s.competition_id, s.season_id, s.match_date, s.team_id, s.opponent_id, s.is_home,
       s.goals_for, s.goals_against,
       CASE WHEN s.goals_for > s.goals_against THEN 'W' WHEN s.goals_for < s.goals_against THEN 'L' ELSE 'D' END AS result,
       COALESCE(sh.shots, 0)             AS shots,
       COALESCE(sh.xg, 0.0)              AS xg,
       COALESCE(sh.goals_from_shots, 0)  AS goals_from_shots,
       COALESCE(a.own_goals_for, 0)      AS own_goals_for,
       p.passes, p.pass_completion, a.pressures, a.events,
       AVG(COALESCE(sh.xg, 0.0)) OVER (
           PARTITION BY s.team_id, s.competition_id, s.season_id
           ORDER BY s.match_date, s.match_id
           ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)   AS xg_rolling_5
FROM sides s
LEFT JOIN shots sh   ON s.match_id = sh.match_id AND s.team_id = sh.team_id
LEFT JOIN passes p   ON s.match_id = p.match_id  AND s.team_id = p.team_id
LEFT JOIN activity a ON s.match_id = a.match_id  AND s.team_id = a.team_id
