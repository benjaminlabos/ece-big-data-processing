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


def apply_language_vocabulary(statements, vocabulary):
    codes = vocabulary.select(
        F.col("language").alias("_entity"), F.lower(F.trim("iso_code")).alias("_iso")
    )
    codes = codes.filter(
        F.col("_iso").rlike(r"^[a-z]{2,3}$")
        & ~F.col("_iso").isin("zxx", "und", "mul", "mis")
    )
    codes = (
        codes.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("_entity").orderBy(F.length("_iso"), F.asc("_iso"))
            ),
        )
        .filter("_rank=1")
        .drop("_rank")
    )
    mapped = statements.withColumnRenamed("language", "_entity")
    unknown = mapped.join(
        codes.select("_entity"), "_entity", "left_anti"
    ).withColumnRenamed("_entity", "language")
    mapped = (
        mapped.join(codes, "_entity")
        .select("tconst", F.col("_iso").alias("language"), "source_url")
        .distinct()
    )
    return mapped, unknown


def analyses(tables, config, languages=None):
    titles, ratings, people = (
        tables[t] for t in ("title.basics", "title.ratings", "name.basics")
    )
    films = titles.filter(
        (F.col("titleType") == "movie")
        & (
            F.col("startYear").isNull()
            | (F.col("startYear") <= config.get("as_of_year", 2026))
        )
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
        .join(rated.select("tconst"), "tconst", "left_semi")
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
        .filter(F.col("nconst").isNotNull())
        .join(rated.select("tconst"), "tconst", "left_semi")
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
        .filter(F.col("nconst").isNotNull())
        .join(films.select("tconst"), "tconst", "left_semi")
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
        .agg(
            F.min("startYear").alias("first_peak_year"),
            F.max("startYear").alias("last_peak_year"),
        )
    )
    q7 = (
        career.join(peaks, "nconst")
        .filter(
            (F.col("last_peak_year") - F.col("first_year"))
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
        .join(rated.select("tconst"), "tconst", "left_semi")
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
        principals.unionByName(crew_roles)
        .groupBy("nconst")
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
    language_rejections = None
    unresolved_languages = None
    language_input_rows = None
    language_valid_unique = None
    language_initial_duplicates = 0
    language_metadata = None
    if languages:
        language_input_rows = language_table.count()
        metadata_path = Path(languages).with_suffix(".manifest.json")
        language_metadata = (
            json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
        )
        if language_metadata.get("sha256") and language_metadata["sha256"] != sha256(
            languages
        ):
            raise ValueError("Original-language snapshot checksum mismatch")
        valid = (
            F.col("tconst").rlike(r"^tt[0-9]{7,}$")
            & F.col("language").isNotNull()
            & (F.length(F.trim("language")) > 0)
            & F.col("source_url").rlike(r"^https?://")
        )
        if (
            language_metadata.get("property")
            == "https://www.wikidata.org/wiki/Property:P364"
        ):
            valid = valid & F.col("language").rlike(
                r"^https?://www\.wikidata\.org/entity/Q[0-9]+$"
            )
        language_rejections = language_table.filter(~F.coalesce(valid, F.lit(False)))
        language_table = language_table.filter(F.coalesce(valid, F.lit(False)))
        valid_rows = language_table.count()
        language_table = language_table.distinct()
        language_valid_unique = language_table.count()
        language_initial_duplicates = valid_rows - language_valid_unique
        if language_metadata.get("vocabulary_path"):
            vocabulary_path = Path(languages).with_name(
                language_metadata["vocabulary_path"]
            )
            if sha256(vocabulary_path) != language_metadata["vocabulary_sha256"]:
                raise ValueError("Language vocabulary checksum mismatch")
            vocabulary = (
                spark.read.schema("language string,iso_code string")
                .option("header", True)
                .option("enforceSchema", False)
                .option("mode", "FAILFAST")
                .csv(str(vocabulary_path))
            )
            language_table, unresolved_languages = apply_language_vocabulary(
                language_table, vocabulary
            )
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    summary = {
        "prepared_summary_sha256": sha256(prepared / "summary.json"),
        "original_languages_sha256": sha256(languages) if languages else None,
        "exploration_module_sha256": sha256(Path(__file__)),
        "scope": source["scope"],
        "config": config,
        "spark_version": spark.version,
        "questions": {},
        "definitions": {
            "popularity": "sum of votes; multi-genre films contribute to each genre",
            "q1_population": "rated feature candidates meeting minimum votes",
            "q4_other_movies": "all eligible directed films with rating below 8",
            "q5": "at least 3 seasons with >=10 known episodes; season 0 excluded; ties accepted existentially; ratings only for scored episodes",
            "q7": "observed feature-candidate film career including unrated titles; all sufficiently voted maximum-rating occurrences within early fraction; plateaus excluded",
            "q8": "career roles across full clean credits, excluding self/archive and unifying actor/actress; performance in eligible rated movies; descriptive not causal",
        },
    }
    if language_table is not None:
        if unresolved_languages is not None:
            unresolved_languages.write.mode("errorifexists").parquet(
                str(stage / "unresolved_language_entities")
            )
        language_rejections.write.mode("errorifexists").parquet(
            str(stage / "rejected_language_statements")
        )
        eligible = (
            tables["title.basics"]
            .filter(
                (F.col("titleType") == "movie")
                & (F.col("runtimeMinutes") >= config["minimum_runtime"])
                & (
                    F.col("startYear").isNull()
                    | (F.col("startYear") <= config.get("as_of_year", 2026))
                )
            )
            .join(tables["title.ratings"], "tconst")
            .filter(F.col("numVotes") >= config["minimum_votes"])
            .select("tconst")
            .distinct()
        )
        summary["original_languages"] = {
            "input_rows": language_input_rows,
            "source_metadata": language_metadata,
            "valid_statements": language_table.count(),
            "rejected_statements": language_rejections.count(),
            "statements_without_identified_iso_language": unresolved_languages.count()
            if unresolved_languages is not None
            else None,
            "eligible_films": eligible.count(),
            "eligible_films_with_language": eligible.join(
                language_table.select("tconst").distinct(), "tconst", "left_semi"
            ).count(),
            "coverage": "observed source statements; not an exhaustive IMDb-language inventory",
        }
        counts = summary["original_languages"]
        counts["duplicate_source_records_collapsed"] = language_initial_duplicates
        counts["duplicate_iso_alias_records_collapsed"] = (
            language_valid_unique
            - (counts["statements_without_identified_iso_language"] or 0)
            - counts["valid_statements"]
        )
        if (
            counts["input_rows"]
            != counts["valid_statements"]
            + counts["rejected_statements"]
            + (counts["statements_without_identified_iso_language"] or 0)
            + counts["duplicate_source_records_collapsed"]
            + counts["duplicate_iso_alias_records_collapsed"]
        ):
            raise AssertionError("Language source row reconciliation failed")
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
