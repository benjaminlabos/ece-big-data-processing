"""Parameterized IMDb batch exploration. Results are provisional until student validation."""

import argparse
import json
import os
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

from .download import sha256
from .exports import single_csv
from .prepare import json_write


def analyses(tables, config, languages=None):
    titles, ratings, people = (
        tables[t] for t in ("title.basics", "title.ratings", "name.basics")
    )
    films = titles.filter(
        (F.col("titleType") == "movie")
        & (F.col("startYear") <= config.get("as_of_year", 2026))
        & (F.col("runtimeMinutes") >= config["minimum_runtime"])
    )
    rated = films.join(ratings, "tconst").filter(
        F.col("numVotes") >= config["minimum_votes"]
    )
    by_genre = rated.withColumn("genre", F.explode("genres"))
    moments = by_genre.groupBy("genre").agg(
        F.avg("runtimeMinutes").alias("mean_runtime"),
        F.stddev_pop("runtimeMinutes").alias("stddev_runtime"),
        F.count("*").alias("genre_films"),
    )
    q1 = (
        by_genre.join(moments, "genre")
        .withColumn(
            "runtime_zscore",
            (F.col("runtimeMinutes") - F.col("mean_runtime"))
            / F.when(F.col("stddev_runtime") > 0, F.col("stddev_runtime")),
        )
        .filter(F.abs("runtime_zscore") >= config["runtime_zscore"])
    )
    yearly = (
        by_genre.filter(F.col("startYear").between(1980, 2025))
        .groupBy("startYear", "genre")
        .agg(
            F.sum("numVotes").alias("genre_votes"),
            F.count("*").alias("film_count"),
            F.avg("averageRating").alias("mean_rating"),
        )
    )
    ranked = yearly.withColumn(
        "genre_rank",
        F.row_number().over(
            Window.partitionBy("startYear").orderBy(
                F.desc("genre_votes"), F.asc("genre")
            )
        ),
    )
    q2 = ranked.filter("genre_rank <= 3")
    q3 = (
        ranked.withColumn(
            "group",
            F.when(F.col("genre_rank") <= 3, "top_three").otherwise("other_genres"),
        )
        .groupBy("startYear", "group")
        .agg(
            F.sum("genre_votes").alias("votes"),
            F.sum("film_count").alias("genre_memberships"),
            F.count("*").alias("genres"),
            (
                F.sum(F.col("mean_rating") * F.col("film_count")) / F.sum("film_count")
            ).alias("mean_rating_per_genre_membership"),
        )
    )
    directors = (
        tables["title.crew"]
        .select("tconst", F.explode("directors").alias("nconst"))
        .distinct()
        .join(rated.select("tconst", "averageRating"), "tconst")
    )
    q4 = (
        directors.groupBy("nconst")
        .agg(
            F.count_if(F.col("averageRating") >= 8).alias("high_rated_films"),
            F.count_if(F.col("averageRating") < 8).alias("other_films"),
            F.avg(F.when(F.col("averageRating") < 8, F.col("averageRating"))).alias(
                "other_mean_rating"
            ),
        )
        .filter(
            (F.col("high_rated_films") == 1)
            & (F.col("other_films") >= 2)
            & (F.col("other_mean_rating") <= 6)
        )
        .join(people.select("nconst", "primaryName"), "nconst")
    )
    episodes = (
        tables["title.episode"]
        .filter((F.col("seasonNumber") > 0) & F.col("episodeNumber").isNotNull())
        .join(ratings, "tconst")
        .filter(F.col("numVotes") >= config["episode_minimum_votes"])
    )
    season_counts = (
        tables["title.episode"]
        .filter(F.col("seasonNumber") > 0)
        .groupBy("parentTconst", "seasonNumber")
        .agg(F.countDistinct("tconst").alias("episodes"))
    )
    series = (
        season_counts.groupBy("parentTconst")
        .agg(
            F.count_if(F.col("episodes") >= 10).alias("qualifying_seasons"),
            F.max("seasonNumber").alias("last_season"),
        )
        .filter("qualifying_seasons >= 3")
        .join(
            titles.filter(F.col("titleType").isin("tvSeries", "tvMiniSeries")).select(
                F.col("tconst").alias("parentTconst")
            ),
            "parentTconst",
        )
    )
    extrema = episodes.groupBy("parentTconst").agg(
        F.min("averageRating").alias("worst_rating"),
        F.max("averageRating").alias("best_rating"),
    )
    marks = (
        episodes.join(extrema, "parentTconst")
        .join(series, "parentTconst")
        .groupBy("parentTconst")
        .agg(
            F.max(
                F.when(
                    (F.col("averageRating") == F.col("worst_rating"))
                    & (F.col("seasonNumber") == F.col("last_season")),
                    1,
                ).otherwise(0)
            ).alias("worst_in_last"),
            F.max(
                F.when(
                    (F.col("averageRating") == F.col("best_rating"))
                    & (F.col("seasonNumber") <= 2),
                    1,
                ).otherwise(0)
            ).alias("best_in_first_two"),
        )
    )
    q5 = (
        marks.filter("worst_in_last = 1 AND best_in_first_two = 1")
        .join(series, "parentTconst")
        .join(extrema, "parentTconst")
    )
    actors = (
        tables["title.principals"]
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst")
        .distinct()
        .join(
            rated.select("tconst", "startYear", "averageRating", "numVotes"), "tconst"
        )
    )
    q6 = None
    if languages is not None:
        valid_languages = (
            languages.filter(
                F.col("language").isNotNull() & (F.length(F.trim("source_url")) > 0)
            )
            .select("tconst", "language", "source_url")
            .distinct()
        )
        q6 = (
            actors.filter(F.col("averageRating") >= config["success_rating"])
            .join(valid_languages, "tconst")
            .groupBy("nconst")
            .agg(
                F.sort_array(F.collect_set("language")).alias("languages"),
                F.countDistinct("language").alias("language_count"),
                F.countDistinct("tconst").alias("successful_films"),
                F.collect_set("source_url").alias("sources"),
            )
            .filter("language_count >= 2 AND successful_films >= 2")
            .withColumn("at_least_three_languages", F.col("language_count") >= 3)
            .join(people.select("nconst", "primaryName"), "nconst")
        )
    career_actors = (
        tables["title.principals"]
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst")
        .distinct()
        .join(films.select("tconst", "startYear"), "tconst")
        .join(ratings, "tconst", "left")
    )
    career = (
        career_actors.groupBy("nconst")
        .agg(
            F.min("startYear").alias("first_year"),
            F.max("startYear").alias("last_year"),
            F.countDistinct("tconst").alias("films"),
            F.max(
                F.when(
                    F.col("numVotes") >= config["minimum_votes"], F.col("averageRating")
                )
            ).alias("peak_rating"),
        )
        .filter(
            (F.col("last_year") - F.col("first_year") >= 20) & (F.col("films") >= 15)
        )
    )
    peaks = (
        career_actors.join(career, "nconst")
        .filter(F.col("numVotes") >= config["minimum_votes"])
        .filter(F.col("averageRating") == F.col("peak_rating"))
        .groupBy("nconst")
        .agg(F.min("startYear").alias("first_peak_year"))
    )
    q7 = (
        career.join(peaks, "nconst")
        .filter(
            (F.col("first_peak_year") - F.col("first_year"))
            <= config["early_career_fraction"]
            * (F.col("last_year") - F.col("first_year"))
        )
        .join(people.select("nconst", "primaryName"), "nconst")
    )
    principals = (
        tables["title.principals"]
        .select("tconst", "nconst", "category")
        .filter(F.col("nconst").isNotNull())
        .filter(~F.col("category").isin("self", "archive_footage", "archive_sound"))
        .withColumn(
            "category",
            F.when(F.col("category").isin("actor", "actress"), "performer").otherwise(
                F.col("category")
            ),
        )
    )
    crew_roles = (
        tables["title.crew"]
        .select("tconst", F.explode("directors").alias("nconst"))
        .withColumn("category", F.lit("director"))
        .unionByName(
            tables["title.crew"]
            .select("tconst", F.explode("writers").alias("nconst"))
            .withColumn("category", F.lit("writer"))
        )
    )
    roles = (
        principals.unionByName(crew_roles)
        .distinct()
        .join(rated.select("tconst", "averageRating", "numVotes"), "tconst")
    )
    projects = (
        roles.groupBy("nconst", "tconst")
        .agg(
            F.sort_array(F.collect_set("category")).alias("roles"),
            F.first("averageRating").alias("averageRating"),
            F.first("numVotes").alias("numVotes"),
        )
        .withColumn("multiple_roles", F.size("roles") >= 2)
    )
    multi_people = (
        roles.groupBy("nconst")
        .agg(F.countDistinct("category").alias("career_roles"))
        .filter("career_roles >= 2")
    )
    q8 = (
        projects.join(multi_people, "nconst")
        .groupBy("nconst", "multiple_roles", "career_roles")
        .agg(
            F.count("*").alias("films"),
            F.avg("averageRating").alias("mean_rating"),
            F.sum("numVotes").alias("votes"),
        )
        .join(people.select("nconst", "primaryName"), "nconst")
    )
    selected = (
        by_genre.filter(F.col("startYear").between(1980, 2025))
        .withColumn(
            "genre_year_rank",
            F.row_number().over(
                Window.partitionBy("startYear", "genre").orderBy(
                    F.desc("averageRating"), F.desc("numVotes"), F.asc("tconst")
                )
            ),
        )
        .filter("genre_year_rank <= 10")
    )
    return {
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "q4": q4,
        "q5": q5,
        "q6": q6,
        "q7": q7,
        "q8": q8,
        "top10_per_genre_year": selected,
    }


def run(spark, prepared, output, config, languages=None):
    prepared, output = Path(prepared).resolve(), Path(output).resolve()
    if output.exists():
        raise FileExistsError(output)
    if output == prepared or prepared in output.parents or output in prepared.parents:
        raise ValueError("Separate input and output required")
    source = json.loads((prepared / "summary.json").read_text())
    if not (prepared / "SUCCESS.json").exists():
        raise ValueError("Prepared snapshot incomplete")
    required = [
        "minimum_runtime",
        "minimum_votes",
        "episode_minimum_votes",
        "runtime_zscore",
        "success_rating",
        "early_career_fraction",
        "student_validated",
    ]
    if any(k not in config for k in required):
        raise ValueError("All methodology parameters must be explicit")
    if not isinstance(config["student_validated"], bool):
        raise ValueError("student_validated must be a boolean")
    if (
        config["minimum_runtime"] <= 0
        or config["minimum_votes"] < 0
        or config["episode_minimum_votes"] < 0
        or config["runtime_zscore"] <= 0
        or not 0 <= config["early_career_fraction"] <= 1
        or not 1 <= config["success_rating"] <= 10
    ):
        raise ValueError("Invalid methodology parameter")
    tables = {
        t: spark.read.parquet(str(prepared / "clean" / t))
        for t in [
            "title.basics",
            "title.ratings",
            "name.basics",
            "title.crew",
            "title.episode",
            "title.principals",
        ]
    }
    language_table = (
        spark.read.schema("tconst string, language string, source_url string")
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .csv(str(languages))
        if languages
        else None
    )
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    summary = {
        "prepared_summary_sha256": sha256(prepared / "summary.json"),
        "original_languages_sha256": sha256(languages) if languages else None,
        "scope": source["scope"],
        "config": config,
        "spark_version": spark.version,
        "questions": {},
        "definitions": {
            "popularity": "sum of votes; multi-genre films contribute to each genre",
            "q1_population": "rated feature candidates meeting minimum votes",
            "q4_other_movies": "all eligible directed films with rating below 8",
            "q5": "at least 3 seasons with >=10 known episodes; season 0 excluded; ties accepted existentially; ratings only for scored episodes",
            "q7": "observed feature-candidate film career including unrated titles; earliest sufficiently voted maximum rating within early fraction",
            "q8": "all principal categories plus directors and writers; descriptive mean, not causal",
        },
    }
    for key, frame in analyses(tables, config, language_table).items():
        if frame is None:
            summary["questions"][key] = {
                "status": "blocked",
                "reason": "Original-language source with provenance required; title.akas.language is not original film language",
            }
            continue
        path = stage / key
        frame.write.mode("errorifexists").parquet(str(path))
        if key == "top10_per_genre_year":
            single_csv(frame.select("tconst").distinct(), stage / "selected_titles.csv")
        summary["questions"][key] = {
            "status": "provisional" if not config["student_validated"] else "computed",
            "rows": spark.read.parquet(str(path)).count(),
        }
    json_write(stage / "summary.json", summary)
    json_write(
        stage / "SUCCESS.json",
        {"scope": source["scope"], "student_validated": config["student_validated"]},
    )
    os.rename(stage, output)
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prepared-dir", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--config", required=True, type=Path)
    p.add_argument("--original-languages", type=Path)
    p.add_argument("--shuffle-partitions", type=int, default=32)
    args = p.parse_args()
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("IMDb exploration")
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(
            json.dumps(
                run(
                    spark,
                    args.prepared_dir,
                    args.output,
                    json.loads(args.config.read_text()),
                    args.original_languages,
                ),
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
