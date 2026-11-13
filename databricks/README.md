# Running the warehouse on Databricks

The same `warehouse` package, run on serverless compute and published to Unity Catalog. Works on
[Databricks Free Edition](https://www.databricks.com/learn/free-edition).

| Notebook | What it does |
|---|---|
| `01_ingest.py` | Creates the schema and two volumes, downloads the raw JSON for the chosen seasons at the pinned commit |
| `02_build.py` | Staging, star schema, SQL marts and the 33 checks, then publishes managed Delta tables with liquid clustering, table and column comments, and primary and foreign keys. Appends every run's check results to `dq_check_results` |
| `03_benchmark.py` | The local benchmark question, raw JSON in a volume vs the Delta table, answers asserted identical |
| `dashboard_queries.sql` | Datasets for an AI/BI dashboard: warehouse health, quality over time, team and player tables |
| `genie_instructions.md` | Tables, instructions and test questions for a Genie space |

## Steps

1. **Workspace → Create → Git folder**, URL `https://github.com/satwik-kotta/event-warehouse-spark`.
2. Open `databricks/01_ingest.py`, attach **Serverless**, Run all. Default seasons: 2022 and 2018 World Cups, Euro 2024 and 2020, La Liga 2015/2016.
3. Run `02_build.py`, then `03_benchmark.py`. Each prints a JSON summary at the end (also saved in the `build` volume).
4. **Catalog Explorer → workspace → event_warehouse**: check the comments, keys and the lineage tab.
5. **Dashboards → Create dashboard**, add the four queries in `dashboard_queries.sql` as datasets, one chart each.
6. **Genie → New**, follow `genie_instructions.md`.
7. Optional: **Jobs & Pipelines → Create job** with the three notebooks as tasks in order, on serverless, scheduled weekly.

## Results

First run on a Databricks trial workspace, serverless compute, 13 Nov 2026. Seasons: 2022 and 2018 World Cups, Euro 2024 and 2020, La Liga 2015/2016.

| | |
|---|---|
| Raw files ingested to the volume | 1,220 event and lineup files, 1,849 MB, 610 matches, 81 s |
| Build (staging, model, marts, checks) | 67 s, 31 s, 18 s, 75 s |
| Published to Unity Catalog | 15 managed Delta tables, 6 with liquid clustering, in 53 s |
| Rows | 2,138,404 events, 605,391 passes, 14,997 shots, 196,785 freeze frame positions, 24,737 lineup rows |
| Quality checks | 33 of 33 passed, 0 warnings. Goals from events equal the published score in all 1,220 team-matches |
| Constraints | 12 primary keys and 12 foreign keys applied, none rejected |
| Benchmark | shots and xG per team, La Liga 2015/2016: 25.4 s from raw JSON in the volume, 0.68 s from the Delta table (38x), identical 20 rows |

The speedup is smaller than the local 175x because the volume holds 610 matches rather than the full 3,400, so the raw path has less to scan.

### Dashboard

An AI/BI dashboard, *Event Warehouse: Health and Results*, built on four datasets from `dashboard_queries.sql`: the latest run's 33 checks, check counts per run over time, xG against goals for every team at the 2022 World Cup (Argentina highest at 13.9 xG), and the top 10 players by xG in each competition-season.

### Genie space

Built on 7 tables (both marts, `fact_shot` and four dimensions). Genie's setup assistant read the table and column comments, added join instructions and the shoot-out rule, and wrote 3 benchmark questions. 2 of 3 passed first time; the third failed because the question did not ask for the shot count its expected SQL returned. After rewording it, 3 of 3 passed.

Two questions asked by hand:

- *Which teams scored more goals than their xG at Euro 2024?* Correct: Spain 15 goals from 10.56 xG (+4.44), then the Netherlands (+2.68) and Switzerland (+2.65).
- *How did Barcelona perform in the Champions League 2015/2016?* One of Genie's own suggested starter questions, but that season is not loaded. Genie correctly said the data is missing, though its explanation then claimed the warehouse does hold Champions League data, which it does not.

What that second answer shows: `dim_competition_season` lists all 80 seasons in the source, including the 75 with no matches loaded, and Genie used it to suggest questions it cannot answer. Two fixes: restrict the dimension to loaded seasons, or add a `has_matches` flag and tell Genie to use it. A product fix would be for starter questions to be checked against the data before they are shown.
