-- End to end check across two source files: goals counted from shot events, plus own goals,
-- must equal the score published in the match file, for every team in every match.
select match_id, team_id, goals_for, goals_from_shots, own_goals_for
from {{ ref('fct_team_match') }}
where goals_from_shots + own_goals_for <> goals_for
