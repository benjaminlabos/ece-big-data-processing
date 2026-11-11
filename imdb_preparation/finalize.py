"""Prepare a reviewed-by-machine, still provisional handoff after the cloud batch."""

import argparse
import json
import os
import shutil
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

from .choose_principals import choose
from .download import sha256
from .exports import read_csv, single_csv
from .prepare import json_write


def verify_published(root):
    root = Path(root).resolve()
    marker = json.loads((root / "VERIFIED.json").read_text())
    if marker.get("scope") != "full":
        raise ValueError("Verified full-scope parent results required")
    skipped = []
    for relative, expected in marker["sha256"].items():
        path = (root / relative).resolve()
        if root not in path.parents:
            raise ValueError("Invalid artifact checksum path")
        # upload-artifact excludes Hadoop's hidden optional CRC sidecars.
        if (
            not path.exists()
            and path.name.startswith(".")
            and path.name.endswith(".crc")
        ):
            skipped.append(relative)
            continue
        if not path.is_file() or sha256(path) != expected:
            raise ValueError(f"Published checksum mismatch: {relative}")
    return skipped


def rankings_for_thresholds(catalog, config):
    spark = catalog.sparkSession
    settings = spark.createDataFrame(
        [
            (duration, votes)
            for duration in sorted({40, 60, 90, int(config["minimum_runtime"])})
            for votes in sorted({100, 1000, 10000, int(config["minimum_votes"])})
        ],
        "minimum_runtime long,minimum_votes long",
    )
    eligible = catalog.crossJoin(F.broadcast(settings)).filter(
        (F.col("runtimeMinutes") >= F.col("minimum_runtime"))
        & (F.col("numVotes") >= F.col("minimum_votes"))
        & F.col("averageRating").isNotNull()
    )
    grouped = eligible.withColumn("genre", F.explode("genres"))
    keys = ["minimum_runtime", "minimum_votes", "startYear", "genre"]
    ranked = grouped.withColumn(
        "genre_year_rank",
        F.row_number().over(
            Window.partitionBy(*keys).orderBy(
                F.desc("averageRating"), F.desc("numVotes"), F.asc("tconst")
            )
        ),
    ).filter("genre_year_rank <= 10")
    return settings, eligible, grouped, ranked


def run(spark, prepared, published, output, baseline_titles, baseline_pairs):
    prepared, published, output = map(Path, (prepared, published, output))
    if output.exists():
        raise FileExistsError(output)
    skipped_crc = verify_published(published)
    if not (prepared / "SUCCESS.json").exists():
        raise ValueError("Seven completed prepared tables required")
    source = json.loads((prepared / "summary.json").read_text())
    analysis = json.loads((published / "analysis/summary.json").read_text())
    if source["scope"] != "full" or len(source["tables"]) != 7:
        raise ValueError("Seven full prepared tables required")
    if analysis["prepared_summary_sha256"] != sha256(prepared / "summary.json"):
        raise ValueError("Prepared cache and published analysis differ")
    config = analysis["config"]
    if config["student_validated"]:
        raise ValueError("This automated handoff preserves a provisional policy")
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    cached = []

    def keep(frame):
        cached.append(frame.cache())
        return cached[-1]

    try:
        catalog = spark.read.parquet(str(published / "integration/movie_catalog"))
        settings, eligible, grouped, ranked = rankings_for_thresholds(catalog, config)
        ranked = keep(ranked)
        current = ranked.filter(
            (F.col("minimum_runtime") == config["minimum_runtime"])
            & (F.col("minimum_votes") == config["minimum_votes"])
        )
        original = spark.read.parquet(str(published / "analysis/top10_per_genre_year"))
        rank_columns = ["startYear", "genre", "genre_year_rank", "tconst"]
        if current.select(*rank_columns).exceptAll(original.select(*rank_columns)).take(
            1
        ) or original.select(*rank_columns).exceptAll(
            current.select(*rank_columns)
        ).take(1):
            raise ValueError("Ranking recomputation differs from parent results")
        selected = keep(
            read_csv(
                spark, published / "analysis/selected_titles.csv", ["tconst"]
            ).distinct()
        )
        ranked_ids = current.select("tconst").distinct()
        if selected.exceptAll(ranked_ids).take(1) or ranked_ids.exceptAll(
            selected
        ).take(1):
            raise ValueError("Selected IDs differ from recomputed ranking")
        all_ids = ranked.select("tconst").distinct()
        credits = keep(
            spark.read.parquet(str(prepared / "clean/title.principals"))
            .select("tconst", "nconst", "ordering", "category")
            .join(F.broadcast(all_ids), "tconst", "left_semi")
        )
        people = keep(
            credits.groupBy("tconst").agg(
                F.countDistinct("nconst").alias("candidate_people")
            )
        )
        primary, reserves = map(
            keep, choose(credits.join(F.broadcast(selected), "tconst", "left_semi"))
        )
        primary_ids = primary.select("tconst", "nconst")
        reserve_ids = reserves.select("tconst", "nconst")
        bounded = keep(primary_ids.unionByName(reserve_ids))
        if bounded.count() != bounded.distinct().count() or bounded.filter(
            ~F.col("tconst").rlike(r"^tt[0-9]{7,}$")
            | ~F.col("nconst").rlike(r"^nm[0-9]{7,}$")
            | F.col("tconst").isNull()
            | F.col("nconst").isNull()
        ).take(1):
            raise ValueError("Invalid or repeated bounded input pair")
        coverage = keep(
            selected.join(people, "tconst", "left")
            .join(
                primary_ids.groupBy("tconst").agg(F.count("*").alias("primary_people")),
                "tconst",
                "left",
            )
            .join(
                reserve_ids.groupBy("tconst").agg(F.count("*").alias("reserve_people")),
                "tconst",
                "left",
            )
            .fillna(0, ["candidate_people", "primary_people", "reserve_people"])
        )
        if coverage.filter(
            (F.col("primary_people") != F.least(F.lit(3), F.col("candidate_people")))
            | (
                F.col("reserve_people")
                != F.least(
                    F.lit(2), F.greatest(F.lit(0), F.col("candidate_people") - 3)
                )
            )
        ).take(1):
            raise ValueError("Bounded person policy does not reconcile")
        coverage = coverage.withColumn(
            "fewer_than_three_candidates", F.col("candidate_people") < 3
        )
        for name, frame in (
            ("primary.csv", primary_ids),
            ("reserves.csv", reserve_ids),
            ("enrichment-input.csv", bounded),
            ("coverage.csv", coverage),
            ("selected-titles.csv", selected),
        ):
            single_csv(frame, stage / name)
        shutil.copyfile(
            published / "analysis/top10_per_genre_year.csv", stage / "top10.csv"
        )
        cfg = ["minimum_runtime", "minimum_votes"]
        keys = cfg + ["startYear", "genre"]
        film_configs = (
            ranked.select(*cfg, "tconst")
            .distinct()
            .join(people, "tconst", "left")
            .fillna(0, ["candidate_people"])
        )
        counts = film_configs.groupBy(*cfg).agg(
            F.count("*").alias("selected_distinct_films"),
            F.count_if(F.col("candidate_people") >= 3).alias(
                "selected_films_with_three_candidates"
            ),
        )
        eligible_counts = eligible.groupBy(*cfg).agg(
            F.count("*").alias("eligible_distinct_films")
        )
        totals = (
            settings.join(eligible_counts, cfg, "left")
            .join(counts, cfg, "left")
            .fillna(0)
        )
        single_csv(totals, stage / "threshold-summary.csv")
        groups = settings.crossJoin(
            catalog.select("startYear", F.explode("genres").alias("genre")).distinct()
        )
        group_counts = grouped.groupBy(*keys).agg(
            F.count("*").alias("eligible_films_in_group")
        )
        selected_counts = (
            ranked.join(people, "tconst", "left")
            .groupBy(*keys)
            .agg(
                F.count("*").alias("selected_places"),
                F.count_if(F.col("candidate_people") >= 3).alias(
                    "selected_places_with_three_candidates"
                ),
            )
        )
        single_csv(
            groups.join(group_counts, keys, "left")
            .join(selected_counts, keys, "left")
            .fillna(0),
            stage / "thresholds-by-year-genre.csv",
        )
        old_titles = read_csv(spark, baseline_titles, ["tconst"]).distinct()
        old_pairs = read_csv(spark, baseline_pairs, ["tconst", "nconst"]).distinct()
        deltas = {}
        for name, fresh, old, cols in (
            ("films", selected, old_titles, ["tconst"]),
            ("pairs", bounded, old_pairs, ["tconst", "nconst"]),
        ):
            for direction, frame in (
                ("added", fresh.join(old, cols, "left_anti")),
                ("removed", old.join(fresh, cols, "left_anti")),
            ):
                name_out = f"{name}-{direction}.csv"
                single_csv(frame, stage / name_out)
                deltas[f"{name}_{direction}"] = frame.count()
        new_people = (
            bounded.select("nconst")
            .distinct()
            .join(old_pairs.select("nconst").distinct(), "nconst", "left_anti")
        )
        single_csv(new_people, stage / "people-added.csv")
        summary = {
            "scope": "full_computation_provisional_handoff",
            "student_validated": False,
            "ready_for_student_review": True,
            "source_prepared_sha256": sha256(prepared / "summary.json"),
            "source_analysis_sha256": sha256(published / "analysis/summary.json"),
            "baseline_titles_sha256": sha256(baseline_titles),
            "baseline_pairs_sha256": sha256(baseline_pairs),
            "config": config,
            "films": selected.count(),
            "primary_pairs": primary_ids.count(),
            "reserve_pairs": reserve_ids.count(),
            "input_pairs": bounded.count(),
            "distinct_people": bounded.select("nconst").distinct().count(),
            "films_with_fewer_than_three_candidates": coverage.filter(
                "fewer_than_three_candidates"
            ).count(),
            "changes_from_previous_inputs": deltas,
            "new_people_to_lookup": new_people.count(),
            "thresholds_compared": totals.count(),
            "optional_unpublished_hadoop_crc_sidecars": skipped_crc,
            "human_review_remaining": [
                "selection thresholds",
                "interpretation of query results",
                "peer review and integration",
            ],
        }
        summary["files_sha256"] = {p.name: sha256(p) for p in stage.glob("*.csv")}
        json_write(stage / "READY_FOR_REVIEW.json", summary)
        os.rename(stage, output)
        return summary
    finally:
        for frame in reversed(cached):
            frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, default=Path("results/cloud-prepared"))
    parser.add_argument("--published", type=Path, default=Path("artifacts/verified"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/handoff"))
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("IMDb overnight handoff")
        .config("spark.sql.shuffle.partitions", 32)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run(
            spark,
            args.prepared,
            args.published,
            args.output,
            Path("validation/selected-titles-full-provisional-2026-11-11.csv"),
            Path("validation/enrichment-input-full-provisional-2026-11-11.csv"),
        )
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
