"""Marts are plain SQL files run against the star schema, so an analyst can read and change them."""
from . import config

MARTS = ["mart_team_match", "mart_player_season"]


def build(spark, tables, out):
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    result = {}
    for name in MARTS:
        sql = (config.ROOT / "sql" / f"{name}.sql").read_text()
        spark.sql(sql).coalesce(1).write.mode("overwrite").parquet(str(out / name))
        result[name] = spark.read.parquet(str(out / name))
        result[name].createOrReplaceTempView(name)
    return result
