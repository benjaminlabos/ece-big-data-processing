"""Compute Q1--Q3 and the film ranking while unrelated tables are still cleaning."""

import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from .download import sha256
from .explore import movie_analyses
from .exports import single_csv
from .prepare import json_write
from .quality import check_reference, clean_values, normalize, partition_keys
from .schema import raw_schema


def run(spark, prepared, snapshot, raw_profile, output, config):
    prepared, snapshot, raw_profile, output = map(
        lambda p: Path(p).resolve(), (prepared, snapshot, raw_profile, output)
    )
    if output.exists():
        raise FileExistsError(output)
    for source in (prepared, snapshot, raw_profile):
        if output == source or source in output.parents or output in source.parents:
            raise ValueError("Separate output required")
    if config.get("student_validated") is not False:
        raise ValueError("This independent subset execution must remain provisional")
    if (
        config["minimum_runtime"] <= 0
        or config["minimum_votes"] < 0
        or config["runtime_zscore"] <= 0
    ):
        raise ValueError("Invalid movie-analysis parameter")
    if not (raw_profile / "PROFILE_SUCCESS.json").exists():
        raise ValueError("Complete raw profile required")
    profile = json.loads((raw_profile / "summary.json").read_text())
    progress_bytes = (prepared / "progress.json").read_bytes()
    progress = json.loads(progress_bytes)
    manifest = json.loads((snapshot / "manifest.json").read_text())
    if profile["manifest_sha256"] != sha256(snapshot / "manifest.json"):
        raise ValueError("Raw profile snapshot mismatch")
    if (
        profile["scope"] != manifest["scope"]
        or profile["scope"] != progress["scope"]
        or profile["scope"] == "sample"
    ):
        raise ValueError("Source scopes inconsistent or incomplete sample")
    if "title.basics" not in progress["completed_tables"]:
        raise ValueError("Completed clean title table required")
    metrics = progress["tables"]["title.basics"]
    if metrics["raw_rows"] != profile["rows"]["title.basics"]:
        raise ValueError("Title source row count differs")
    titles = spark.read.parquet(str(prepared / "clean" / "title.basics"))
    if titles.count() != metrics["clean_rows"]:
        raise ValueError("Clean title row count differs")
    source_metadata = manifest["files"]["title.ratings"]
    ratings_path = (snapshot / source_metadata["path"]).resolve()
    if (
        ratings_path.parent != snapshot
        or sha256(ratings_path) != source_metadata["sha256"]
    ):
        raise ValueError("Ratings source checksum mismatch")
    raw = (
        spark.read.schema(raw_schema("title.ratings"))
        .option("header", True)
        .option("sep", "\t")
        .option("quote", "")
        .option("escape", "")
        .option("mode", "FAILFAST")
        .option("enforceSchema", False)
        .csv(str(ratings_path))
    )
    raw_count = raw.count()
    if raw_count != profile["rows"]["title.ratings"]:
        raise ValueError("Ratings source row count differs")
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    cached = []

    def cache(frame):
        cached.append(frame.persist(StorageLevel.DISK_ONLY))
        return cached[-1]

    try:
        distinct, valid, rejected = partition_keys("title.ratings", normalize(raw))
        distinct = cache(distinct)
        distinct_count = distinct.count()
        valid = cache(valid)
        valid_count = valid.count()
        rejected.write.parquet(str(stage / "ratings_key_quarantine"))
        typed, scalar_events = clean_values(
            "title.ratings", valid, config.get("as_of_year", 2026)
        )
        typed = cache(typed)
        ratings, reference_events, orphans = check_reference(
            "title.ratings", typed, "tconst", titles, "tconst", False, True
        )
        ratings = cache(ratings)
        clean_count = ratings.count()
        orphan_count = orphans.count()
        if (
            raw_count
            != clean_count
            + (raw_count - distinct_count)
            + (distinct_count - valid_count)
            + orphan_count
        ):
            raise AssertionError("Rating row reconciliation failed")
        ratings.write.parquet(str(stage / "clean_ratings"))
        scalar_events.unionByName(reference_events).write.parquet(
            str(stage / "ratings_anomalies")
        )
        orphans.write.parquet(str(stage / "ratings_reference_quarantine"))
        summary = {
            "scope": profile["scope"] + "_title_tables_subset",
            "complete_preparation_validated": False,
            "source_title_status": "completed table from preparation progress; other tables are not certified by this run",
            "source_progress_sha256": hashlib.sha256(progress_bytes).hexdigest(),
            "source_manifest_sha256": profile["manifest_sha256"],
            "module_sha256": sha256(Path(__file__)),
            "exploration_module_sha256": sha256(Path(__file__).with_name("explore.py")),
            "spark_version": spark.version,
            "title_metrics": metrics,
            "ratings": {
                "raw_rows": raw_count,
                "clean_rows": clean_count,
                "duplicates_removed": raw_count - distinct_count,
                "key_quarantined": distinct_count - valid_count,
                "reference_quarantined": orphan_count,
            },
            "config": config,
            "questions": {},
        }
        _, _, queries = movie_analyses(titles, ratings, config)
        for key, frame in queries.items():
            frame.write.parquet(str(stage / key))
            written = spark.read.parquet(str(stage / key))
            count = written.count()
            summary["questions"][key] = {"status": "provisional", "rows": count}
            order = [
                c for c in ("startYear", "genre", "tconst") if c in written.columns
            ]
            preview = written.orderBy(*order) if order else written
            preview_dir = stage / "previews"
            preview_dir.mkdir(exist_ok=True)
            json_write(
                preview_dir / (key + ".json"),
                [r.asDict(recursive=True) for r in preview.limit(20).collect()],
            )
            if key == "top10_per_genre_year":
                selected = written.select("tconst").distinct()
                single_csv(selected.orderBy("tconst"), stage / "selected_titles.csv")
                single_csv(
                    written.select(
                        "startYear",
                        "genre",
                        "genre_year_rank",
                        "tconst",
                        "primaryTitle",
                        "runtimeMinutes",
                        "averageRating",
                        "numVotes",
                    ).orderBy("startYear", "genre", "genre_year_rank"),
                    stage / "top10_per_genre_year.csv",
                )
                summary["distinct_selected_films"] = selected.count()
            json_write(stage / "progress.json", summary)
            print(f"{key}: {count} rows written", flush=True)
        json_write(stage / "summary.json", summary)
        json_write(
            stage / "PROVISIONAL_SUCCESS.json",
            {"scope": summary["scope"], "student_validated": False},
        )
        os.rename(stage, output)
        return summary
    finally:
        for frame in cached:
            frame.unpersist()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("prepared", "snapshot", "raw-profile", "output", "config"):
        p.add_argument("--" + name, required=True, type=Path)
    args = p.parse_args()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("IMDb title analysis subset")
        .config("spark.sql.shuffle.partitions", 16)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(
            json.dumps(
                run(
                    spark,
                    args.prepared,
                    args.snapshot,
                    args.raw_profile,
                    args.output,
                    json.loads(args.config.read_text()),
                ),
                indent=2,
            )
        )
    finally:
        spark.stop()
