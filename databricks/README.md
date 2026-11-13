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

Filled in from the printed summaries after the first run.
