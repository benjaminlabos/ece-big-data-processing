"""Audit materialized query outputs and export bounded technical review tables."""

import argparse
import json
import os
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

from .download import sha256
from .exports import single_csv
from .prepare import json_write

QUESTIONS = [f"q{i}" for i in range(1, 9)] + ["top10_per_genre_year"]


def audit_frame(name, frame, config):
    """Check output invariants without re-running the source query."""
    year = F.col("startYear").between(1980, 2025)

    def rating(column):
        return F.col(column).between(1, 10)

    def person():
        return F.col("nconst").rlike(r"^nm[0-9]{7,}$")

    if name == "q1":
        keys = ["tconst", "genre"]
        valid = (
            F.col("tconst").rlike(r"^tt[0-9]{7,}$")
            & (F.length("genre") > 0)
            & (F.col("runtimeMinutes") >= config["minimum_runtime"])
            & (F.col("stddev_runtime") > 0)
            & (F.col("genre_films") > 0)
            & (F.abs("runtime_zscore") + 1e-9 >= config["runtime_zscore"])
        )
    elif name == "q2":
        keys = ["startYear", "genre"]
        valid = (
            year
            & (F.length("genre") > 0)
            & F.col("genre_rank").between(1, 3)
            & (F.col("genre_votes") >= 0)
            & (F.col("film_count") > 0)
            & rating("mean_rating")
        )
    elif name == "q3":
        keys = ["startYear", "group"]
        valid = (
            year
            & F.col("group").isin("top_three", "other_genres")
            & (F.col("votes") >= 0)
            & (F.col("genres") > 0)
            & (F.col("genre_memberships") >= F.col("genres"))
            & rating("mean_rating_per_genre_membership")
        )
    elif name == "q4":
        keys = ["nconst"]
        valid = (
            person()
            & (F.col("high_rated_films") == 1)
            & (F.col("other_films") >= 2)
            & F.col("other_mean_rating").between(1, 6)
        )
    elif name == "q5":
        keys = ["parentTconst"]
        valid = (
            F.col("parentTconst").rlike(r"^tt[0-9]{7,}$")
            & (F.col("qualifying_seasons") >= 3)
            & (F.col("last_season") >= 3)
            & (F.col("worst_in_last") == 1)
            & (F.col("best_in_first_two") == 1)
            & rating("worst_rating")
            & rating("best_rating")
            & (F.col("worst_rating") <= F.col("best_rating"))
        )
    elif name == "q6":
        keys = ["nconst"]
        valid = (
            person()
            & (F.col("language_count") >= 2)
            & (F.col("successful_films") >= 2)
            & (F.size(F.array_distinct("languages")) == F.col("language_count"))
            & (F.size("languages") == F.col("language_count"))
            & (F.size("sources") > 0)
            & ~F.exists("sources", lambda source: ~source.rlike(r"^https?://"))
            & (F.col("at_least_three_languages") == (F.col("language_count") >= 3))
        )
    elif name == "q7":
        keys = ["nconst"]
        valid = (
            person()
            & (F.col("last_year") - F.col("first_year") >= 20)
            & (F.col("films") >= 15)
            & rating("peak_rating")
            & (F.col("first_peak_year") >= F.col("first_year"))
            & (F.col("last_peak_year") >= F.col("first_peak_year"))
            & (F.col("last_peak_year") <= F.col("last_year"))
            & (
                F.col("last_peak_year") - F.col("first_year")
                <= config["early_career_fraction"]
                * (F.col("last_year") - F.col("first_year"))
                + 1e-9
            )
        )
    elif name == "q8":
        keys = ["nconst", "multiple_roles"]
        valid = (
            person()
            & F.col("multiple_roles").isNotNull()
            & (F.col("career_roles") >= 2)
            & (F.col("films") > 0)
            & rating("mean_rating")
            & (F.col("votes") >= 0)
        )
    elif name == "top10_per_genre_year":
        keys = ["startYear", "genre", "tconst"]
        valid = (
            year
            & F.col("tconst").rlike(r"^tt[0-9]{7,}$")
            & (F.length("genre") > 0)
            & F.col("genre_year_rank").between(1, 10)
            & rating("averageRating")
            & (F.col("runtimeMinutes") >= config["minimum_runtime"])
            & (F.col("numVotes") >= config["minimum_votes"])
        )
    else:
        raise ValueError(f"Unknown query: {name}")
    if frame.filter(~F.coalesce(valid, F.lit(False))).take(1):
        raise ValueError(f"{name}: result violates its declared query invariants")
    if frame.groupBy(*keys).count().filter("count > 1").take(1):
        raise ValueError(f"{name}: duplicate result key")
    if name in ("q2", "top10_per_genre_year"):
        group = ["startYear"] if name == "q2" else ["startYear", "genre"]
        rank = "genre_rank" if name == "q2" else "genre_year_rank"
        order = (
            [F.desc("genre_votes"), F.asc("genre")]
            if name == "q2"
            else [F.desc("averageRating"), F.desc("numVotes"), F.asc("tconst")]
        )
        checked = frame.withColumn(
            "_expected_rank",
            F.row_number().over(Window.partitionBy(*group).orderBy(*order)),
        )
        if checked.filter(F.col(rank) != F.col("_expected_rank")).take(1):
            raise ValueError(f"{name}: inconsistent rank or tie break")
    return keys


def run(spark, analysis, prepared, output, preview_rows=100):
    analysis, prepared, output = map(
        lambda p: Path(p).resolve(), (analysis, prepared, output)
    )
    if output.exists():
        raise FileExistsError(output)
    if preview_rows < 1:
        raise ValueError("Positive preview row limit required")
    if any(
        output == source or source in output.parents or output in source.parents
        for source in (analysis, prepared)
    ):
        raise ValueError("Review output must be separate from its source directories")
    source = json.loads((prepared / "summary.json").read_text())
    summary = json.loads((analysis / "summary.json").read_text())
    for directory in (prepared, analysis):
        marker = json.loads((directory / "SUCCESS.json").read_text())
        if marker["scope"] != source["scope"]:
            raise ValueError("Success marker scope mismatch")
    if summary["scope"] != source["scope"] or summary[
        "prepared_summary_sha256"
    ] != sha256(prepared / "summary.json"):
        raise ValueError("Analysis/preparation provenance mismatch")
    if set(summary["questions"]) != set(QUESTIONS):
        raise ValueError("Eight queries and ranking required")
    if any(
        q["status"] not in ("computed", "provisional")
        for q in summary["questions"].values()
    ):
        raise ValueError("Incomplete query output")
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    frames = {}
    report = {
        "scope": source["scope"],
        "validation": "materialized_query_invariants_not_human_interpretation",
        "student_validated": summary["config"]["student_validated"],
        "spark_version": spark.version,
        "analysis_summary_sha256": sha256(analysis / "summary.json"),
        "prepared_summary_sha256": sha256(prepared / "summary.json"),
        "questions": {},
    }
    for name in QUESTIONS:
        frame = spark.read.parquet(str(analysis / name))
        count = frame.count()
        if count != summary["questions"][name]["rows"]:
            raise ValueError(f"{name}: declared row count differs from Parquet")
        keys = audit_frame(name, frame, summary["config"])
        frames[name] = frame
        preview = frame.orderBy(*keys).limit(preview_rows)
        json_write(
            stage / f"{name}-preview.json",
            [r.asDict(recursive=True) for r in preview.collect()],
        )
        flat = preview.select(
            *[
                F.to_json(F.col(field.name)).alias(field.name)
                if isinstance(field.dataType, (ArrayType, MapType, StructType))
                else F.col(field.name)
                for field in preview.schema.fields
            ]
        )
        single_csv(flat, stage / f"{name}-preview.csv")
        report["questions"][name] = {
            "rows": count,
            "preview_rows": min(count, preview_rows),
            "columns": frame.columns,
            "invariants": "passed",
        }
        print(f"{name}: {count} rows audited", flush=True)
    expected = (
        frames["q2"]
        .groupBy("startYear")
        .agg(
            F.sum("genre_votes").alias("_votes"),
            F.sum("film_count").alias("_memberships"),
            F.count("*").alias("_genres"),
            (
                F.sum(F.col("mean_rating") * F.col("film_count")) / F.sum("film_count")
            ).alias("_rating"),
        )
    )
    actual = frames["q3"].filter("`group` = 'top_three'")
    if actual.select("startYear").exceptAll(expected.select("startYear")).take(
        1
    ) or expected.select("startYear").exceptAll(actual.select("startYear")).take(1):
        raise ValueError("Q2/Q3 year sets differ")
    if (
        actual.join(expected, "startYear")
        .filter(
            (F.col("votes") != F.col("_votes"))
            | (F.col("genre_memberships") != F.col("_memberships"))
            | (F.col("genres") != F.col("_genres"))
            | (
                F.abs(F.col("mean_rating_per_genre_membership") - F.col("_rating"))
                > 1e-9
            )
        )
        .take(1)
    ):
        raise ValueError("Q2/Q3 aggregate reconciliation failed")
    selected = (
        spark.read.option("header", True)
        .csv(str(analysis / "selected_titles.csv"))
        .select("tconst")
    )
    ranked = frames["top10_per_genre_year"].select("tconst").distinct()
    if selected.exceptAll(ranked).take(1) or ranked.exceptAll(selected).take(1):
        raise ValueError("Selected titles differ from ranking")
    report.update(
        status="passed",
        q2_q3_reconciled=True,
        selection_reconciled=True,
        note="CSV/JSON files are bounded previews; full outputs remain in source Parquet.",
    )
    report["files_sha256"] = {
        p.name: sha256(p)
        for p in stage.iterdir()
        if p.is_file() and not p.name.startswith(".")
    }
    json_write(stage / "QUERY_REVIEW.json", report)
    os.rename(stage, output)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--analysis", type=Path, default=Path("artifacts/verified/analysis")
    )
    parser.add_argument("--prepared", type=Path, default=Path("results/cloud-prepared"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/query-review"))
    parser.add_argument("--preview-rows", type=int, default=100)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("IMDb materialized output review")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(
            json.dumps(
                run(
                    spark, args.analysis, args.prepared, args.output, args.preview_rows
                ),
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
