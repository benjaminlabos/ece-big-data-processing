"""Compare IMDb ratings within director–performer pairs across release years."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F


TABLES = ("title.basics", "title.crew", "title.principals", "title.ratings")
PAIR = ("director_id", "performer_id")
COUNT_COLUMNS = ("pairs", "first_year_pair_films", "later_year_pair_films")


def build_history(titles, crew, principals, ratings, end_year):
    movies = titles.where(
        (F.col("titleType") == "movie")
        & (F.col("startYear").isNull() | (F.col("startYear") <= end_year))
    ).select("tconst", F.col("startYear").alias("year"))
    directors = crew.select("tconst", F.explode("directors").alias("director_id"))
    performers = principals.where(F.col("category").isin("actor", "actress")).select(
        "tconst", F.col("nconst").alias("performer_id")
    )
    pair_films = (
        movies.join(directors, "tconst").join(performers, "tconst")
        .where(F.col("director_id").isNotNull() & F.col("performer_id").isNotNull())
        .select(*PAIR, "tconst", "year").distinct()
    )
    pair_window = Window.partitionBy(*PAIR)
    # All films in the same year share a rank. Unknown years never enter the history.
    year_rank = F.when(
        F.col("year").isNotNull(),
        F.dense_rank().over(pair_window.orderBy(F.col("year").asc_nulls_last())),
    )
    return (
        pair_films.withColumn("year_rank", year_rank)
        .withColumn("history_years", F.max("year_rank").over(pair_window))
        .join(ratings.select("tconst", "averageRating", "numVotes"), "tconst", "left")
    )


def compare(spark, history, start_year, votes, minimum_years):
    vote_thresholds = spark.createDataFrame([(v,) for v in votes], "min_votes INT")
    year_thresholds = spark.createDataFrame(
        [(y,) for y in minimum_years], "min_history_years INT"
    )
    first_year = F.col("year_rank") == 1
    later_year = F.col("year_rank") > 1
    pair_ratings = (
        history.where((F.col("year") >= start_year) & F.col("averageRating").isNotNull())
        .join(F.broadcast(vote_thresholds), F.col("numVotes") >= F.col("min_votes"))
        .groupBy(*PAIR, "min_votes")
        .agg(
            F.max("history_years").alias("history_years"),
            F.count_if(first_year).alias("first_year_pair_films"),
            F.count_if(later_year).alias("later_year_pair_films"),
            F.avg(F.when(first_year, F.col("averageRating"))).alias("first_mean"),
            F.avg(F.when(later_year, F.col("averageRating"))).alias("later_mean"),
        )
        .where((F.col("first_year_pair_films") > 0) & (F.col("later_year_pair_films") > 0))
        .join(F.broadcast(year_thresholds), F.col("history_years") >= F.col("min_history_years"))
    )
    keys = ["min_votes", "min_history_years"]
    summaries = pair_ratings.groupBy(*keys).agg(
        F.count("*").alias("pairs"),
        F.sum("first_year_pair_films").alias("first_year_pair_films"),
        F.sum("later_year_pair_films").alias("later_year_pair_films"),
        F.avg("first_mean").alias("mean_first_year_rating_per_pair"),
        F.avg("later_mean").alias("mean_later_year_rating_per_pair"),
        F.avg(F.col("later_mean") - F.col("first_mean")).alias("mean_within_pair_change"),
    )
    return (
        vote_thresholds.crossJoin(year_thresholds)
        .join(summaries, keys, "left").fillna(0, subset=list(COUNT_COLUMNS))
    )


def run(spark, clean_dir, output, args):
    clean_dir = clean_dir.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Choose a new output directory: {output}")
    if output == clean_dir or output.is_relative_to(clean_dir):
        raise ValueError("Output must be outside the input directory")
    manifest_sha256 = None
    if args.input_manifest:
        manifest = args.input_manifest.read_bytes()
        if json.loads(manifest).get("scope") != args.scope:
            raise ValueError("The analysis scope must match the input manifest")
        manifest_sha256 = hashlib.sha256(manifest).hexdigest()
    tables = [spark.read.parquet(str(clean_dir / table)) for table in TABLES]
    history = build_history(*tables, args.end_year).persist(StorageLevel.DISK_ONLY)
    is_self = F.col("director_id") == F.col("performer_id")
    population = history if args.include_self else history.where(~is_self)
    in_period = F.col("year") >= args.start_year
    try:
        diagnostics = history.agg(
            F.count("*").alias("movie_pair_films_before_self_policy"),
            F.count_if(is_self).alias("self_pair_films"),
        ).first().asDict()
        diagnostics.update(population.agg(
            F.count_if(F.col("year").isNull()).alias("pair_films_unknown_year"),
            F.count_if(in_period & F.col("averageRating").isNull())
            .alias("pair_films_in_period_without_rating"),
            F.count_if(in_period & (F.col("year_rank") == 1))
            .alias("first_year_pair_films_in_period_before_vote_filter"),
            F.count_if(in_period & (F.col("year_rank") > 1))
            .alias("later_year_pair_films_in_period_before_vote_filter"),
        ).first().asDict())
        staging = output.with_name(f"{output.name}.incomplete-{uuid.uuid4().hex[:8]}")
        staging.mkdir(parents=True)
        result_path = staging / "comparisons"
        table = compare(spark, population, args.start_year, args.min_votes, args.min_history_years)
        table.write.parquet(str(result_path))
        rows = [
            row.asDict()
            for row in spark.read.parquet(str(result_path))
            .orderBy("min_votes", "min_history_years").collect()
        ]
        summary = {
            "scope": args.scope,
            "student_validated": False,
            "spark_version": spark.version,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_manifest_sha256": manifest_sha256,
            "start_year": args.start_year,
            "end_year": args.end_year,
            "include_self": args.include_self,
            "diagnostics": diagnostics,
            "comparisons": rows,
        }
        (staging / "summary.json").write_text(
            json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )
        os.rename(staging, output)
        return summary
    finally:
        history.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clean-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scope", required=True, choices=("fixture", "sample", "full"))
    parser.add_argument("--input-manifest", type=Path)
    parser.add_argument("--start-year", type=int, default=1980)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--min-votes", type=int, nargs="+", default=[0, 100, 1000])
    parser.add_argument("--min-history-years", type=int, nargs="+", default=[2, 3])
    parser.add_argument("--include-self", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.start_year <= args.end_year <= 9999:
        parser.error("Expected 1 <= start-year <= end-year <= 9999")
    if min(args.min_votes) < 0 or min(args.min_history_years) < 2:
        parser.error("Vote minima must be nonnegative and history minima must be at least two")
    args.min_votes = sorted(set(args.min_votes))
    args.min_history_years = sorted(set(args.min_history_years))
    spark = SparkSession.builder.appName("IMDb repeated collaborations").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run(spark, args.clean_dir, args.output, args)
        print(json.dumps(summary, indent=2, allow_nan=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
