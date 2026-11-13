-- Datasets for the AI/BI dashboard. Replace workspace.event_warehouse if you used another catalog or schema.

-- 1. Warehouse health: latest run's checks
SELECT check, severity, passed, failures, detail
FROM workspace.event_warehouse.dq_check_results
WHERE run_id = (SELECT max_by(run_id, run_at) FROM workspace.event_warehouse.dq_check_results)
ORDER BY passed, severity, check;

-- 2. Data quality over time (one point per run)
SELECT run_at, SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN NOT passed AND severity = 'warn' THEN 1 ELSE 0 END) AS warnings,
       SUM(CASE WHEN NOT passed AND severity = 'error' THEN 1 ELSE 0 END) AS errors
FROM workspace.event_warehouse.dq_check_results
GROUP BY run_at ORDER BY run_at;

-- 3. Team season table: record, goals and xG for and against
SELECT cs.competition_name, cs.season_name, t.team_name,
       COUNT(*) AS matches,
       SUM(CASE WHEN tm.result = 'W' THEN 1 ELSE 0 END) AS wins,
       SUM(tm.goals_for) AS goals_for, SUM(tm.goals_against) AS goals_against,
       ROUND(SUM(tm.xg), 2) AS xg_for,
       ROUND(SUM(tm.goals_for) - SUM(tm.xg), 2) AS goals_minus_xg
FROM workspace.event_warehouse.mart_team_match tm
JOIN workspace.event_warehouse.dim_team t ON tm.team_id = t.team_id
JOIN workspace.event_warehouse.dim_competition_season cs USING (competition_id, season_id)
GROUP BY ALL
ORDER BY competition_name, season_name, xg_for DESC;

-- 4. Top players by expected goals
SELECT cs.competition_name, cs.season_name, p.known_as AS player, ps.*
FROM workspace.event_warehouse.mart_player_season ps
JOIN workspace.event_warehouse.dim_player p ON ps.player_id = p.player_id
JOIN workspace.event_warehouse.dim_competition_season cs USING (competition_id, season_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.competition_id, ps.season_id ORDER BY ps.xg DESC) <= 10;
