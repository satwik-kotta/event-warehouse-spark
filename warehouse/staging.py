"""Raw JSON -> typed staging tables. This is the only step that touches the raw files."""
from pyspark.sql import functions as F

from . import schemas

def match_id_from_filename():
    return F.regexp_extract(F.input_file_name(), r"(\d+)\.json$", 1).cast("int")


def read_events(spark, raw):
    """One row per event. Each file is a JSON array named <match_id>.json."""
    return (spark.read.schema(schemas.EVENT).option("multiLine", True).option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(str(raw / "events" / "*.json")).withColumn("match_id", match_id_from_filename()))


def read_matches(spark, raw):
    return spark.read.schema(schemas.MATCH).option("multiLine", True).json(str(raw / "matches" / "*" / "*.json"))


def read_competitions(spark, raw):
    return spark.read.schema(schemas.COMPETITION).option("multiLine", True).json(str(raw / "competitions.json"))


def read_lineups(spark, raw):
    return (spark.read.schema(schemas.LINEUP).option("multiLine", True)
            .json(str(raw / "lineups" / "*.json")).withColumn("match_id", match_id_from_filename()))


def stage(spark, raw, out):
    """Write the staging layer and return it re-read from Parquet, so nothing downstream re-parses JSON."""
    stg = out / "staging"
    matches = read_matches(spark, raw).select(
        "*", F.col("competition.competition_id").alias("competition_id"), F.col("season.season_id").alias("season_id"))
    matches.write.mode("overwrite").parquet(str(stg / "matches"))
    read_competitions(spark, raw).write.mode("overwrite").parquet(str(stg / "competitions"))
    read_lineups(spark, raw).write.mode("overwrite").parquet(str(stg / "lineups"))

    keys = F.broadcast(spark.read.parquet(str(stg / "matches")).select("match_id", "competition_id", "season_id"))
    events = read_events(spark, raw).join(keys, "match_id", "left")
    (events.repartition("competition_id", "season_id").write.mode("overwrite")
     .partitionBy("competition_id", "season_id").parquet(str(stg / "events")))
    tables = {name: spark.read.parquet(str(stg / name)) for name in ("events", "matches", "competitions", "lineups")}
    # Event files whose match is missing from the match listing cannot be placed in a competition or
    # season. They stay in staging (in the null partition) and are kept out of the star schema.
    everything = tables["events"]
    tables["events"] = everything.where(F.col("competition_id").isNotNull())
    tables["events_unmatched"] = everything.where(F.col("competition_id").isNull())
    return tables
