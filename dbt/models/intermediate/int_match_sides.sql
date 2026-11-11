-- Each match seen from both sides: one row per team per match.
select match_id, competition_id, season_id, match_date, home_team_id as team_id, away_team_id as opponent_id,
       true as is_home, home_score as goals_for, away_score as goals_against
from {{ ref('stg_matches') }}
union all
select match_id, competition_id, season_id, match_date, away_team_id, home_team_id,
       false, away_score, home_score
from {{ ref('stg_matches') }}
