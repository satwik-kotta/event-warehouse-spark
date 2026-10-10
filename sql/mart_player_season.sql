-- One row per player per competition-season, ranked on expected goals within that season.
WITH played AS (
    SELECT competition_id, season_id, player_id, COUNT(*) AS matches, SUM(CASE WHEN is_starter THEN 1 ELSE 0 END) AS starts
    FROM fact_lineup
    WHERE played
    GROUP BY competition_id, season_id, player_id
),
shooting AS (
    SELECT competition_id, season_id, player_id,
           COUNT(*) AS shots, SUM(xg) AS xg, SUM(CASE WHEN is_goal THEN 1 ELSE 0 END) AS goals
    FROM fact_shot
    WHERE period <> 5
    GROUP BY competition_id, season_id, player_id
),
passing AS (
    SELECT competition_id, season_id, player_id,
           COUNT(*) AS passes,
           AVG(CAST(CAST(is_complete AS INT) AS DOUBLE)) AS pass_completion,
           SUM(CASE WHEN is_shot_assist OR is_goal_assist THEN 1 ELSE 0 END) AS key_passes
    FROM fact_pass
    GROUP BY competition_id, season_id, player_id
)
SELECT pl.competition_id, pl.season_id, pl.player_id, pl.matches, pl.starts,
       COALESCE(s.shots, 0) AS shots, COALESCE(s.goals, 0) AS goals, COALESCE(s.xg, 0.0) AS xg,
       COALESCE(s.goals, 0) - COALESCE(s.xg, 0.0) AS goals_minus_xg,
       COALESCE(p.passes, 0) AS passes, p.pass_completion, COALESCE(p.key_passes, 0) AS key_passes,
       RANK() OVER (PARTITION BY pl.competition_id, pl.season_id ORDER BY COALESCE(s.xg, 0.0) DESC) AS xg_rank
FROM played pl
LEFT JOIN shooting s ON pl.competition_id = s.competition_id AND pl.season_id = s.season_id AND pl.player_id = s.player_id
LEFT JOIN passing p  ON pl.competition_id = p.competition_id AND pl.season_id = p.season_id AND pl.player_id = p.player_id
