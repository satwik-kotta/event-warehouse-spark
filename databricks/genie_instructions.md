# Genie space setup

**Tables to add:** `mart_team_match`, `mart_player_season`, `fact_shot`, `dim_match`, `dim_team`, `dim_player`, `dim_competition_season`.
Start with the marts. Genie answers better from a few wide, well documented tables than from the whole star schema.

**General instructions (paste into the space):**

```
This space covers football matches from StatsBomb open data: two World Cups, two Euros and La Liga 2015/2016.
- xg means expected goals, the probability a shot is scored. Sum it to get a team's or player's chances created.
- Penalty shoot-outs are period 5 in fact_shot and never count towards goals or xG. Exclude them unless asked.
- For team questions start from mart_team_match. Each match appears twice, once from each team's side.
- For player questions start from mart_player_season and join dim_player on player_id, using known_as as the name.
- Competition and season names are in dim_competition_season, joined on competition_id and season_id.
- "Overperformed" means goals minus xG is positive.
```

**Sample questions to save in the space (and to test it):**

1. Which team created the most xG at the 2022 World Cup?
2. Which teams scored more goals than their xG at Euro 2024, and by how much?
3. Who were the top 5 players by xG in La Liga 2015/2016?
4. How did Argentina's rolling 5 match xG change through the 2022 World Cup?
5. What share of shots in the 2018 World Cup were taken under pressure, and how did their conversion compare?

For each one, check the SQL Genie wrote. Where it is wrong, add the correct query as a trusted example. Note which
questions needed fixing: that list is the most useful thing to bring to a product interview.
