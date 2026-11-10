"""Spark interfaces from prepared IMDb data to streaming and enrichment.

This module does not rank films or collect Wikipedia content. A title selection
and a Wikipedia reference snapshot, when provided, remain explicit inputs.
"""

import argparse
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from .download import sha256
from .prepare import json_write

REFERENCE_COLUMNS = ["wiki", "title", "imdb_id", "wikidata_id"]
REQUIRED = {
    "title.basics": [
        "tconst",
        "titleType",
        "primaryTitle",
        "startYear",
        "runtimeMinutes",
        "genres",
    ],
    "name.basics": ["nconst"],
    "title.principals": ["tconst", "ordering", "nconst", "category"],
    "title.ratings": ["tconst", "averageRating", "numVotes"],
}


def unique_parent(frame, key, prefix, label):
    invalid = F.col(key).isNull() | ~F.col(key).rlike(r"^" + prefix + r"[0-9]{7,}$")
    if frame.filter(invalid).take(1):
        raise ValueError(f"Invalid parent key: {label}.{key}")
    if frame.groupBy(key).count().filter("count > 1").take(1):
        raise ValueError(
            f"Duplicate parent key: {label}.{key}; refusing a multiplying join"
        )


def load_prepared(spark, prepared_dir):
    prepared_dir = Path(prepared_dir).resolve()
    success = json.loads((prepared_dir / "SUCCESS.json").read_text(encoding="utf-8"))
    summary = json.loads((prepared_dir / "summary.json").read_text(encoding="utf-8"))
    if (
        summary.get("scope") not in ("full", "sample", "fixture")
        or success.get("scope") != summary["scope"]
    ):
        raise ValueError("Prepared scope is missing or inconsistent with SUCCESS.json")
    frames = {}
    for table, columns in REQUIRED.items():
        if table not in summary.get("tables", {}):
            raise ValueError(f"Prepared summary is missing {table}")
        frame = spark.read.parquet(str(prepared_dir / "clean" / table))
        if not set(columns).issubset(frame.columns):
            raise ValueError(f"Missing columns in prepared {table}: {columns}")
        frames[table] = frame.select(*columns)
    for table in ("title.basics", "name.basics", "title.ratings"):
        key, prefix = ("nconst", "nm") if table == "name.basics" else ("tconst", "tt")
        unique_parent(frames[table], key, prefix, table)
    # Enforce the numeric contracts before comparisons (no lexical year ranges).
    expected_types = {
        "title.basics": {
            "startYear": "bigint",
            "runtimeMinutes": "bigint",
            "genres": "array<string>",
        },
        "title.ratings": {"averageRating": "double", "numVotes": "bigint"},
    }
    for table, columns in expected_types.items():
        for column, expected in columns.items():
            if frames[table].schema[column].dataType.simpleString() != expected:
                raise ValueError(f"Expected {table}.{column} to have type {expected}")
    return frames, summary


def movie_catalog(titles, ratings):
    """A candidate base; movie is an IMDb type, with no runtime/ranking threshold."""
    return (
        titles.filter(
            (F.col("titleType") == "movie") & F.col("startYear").between(1980, 2025)
        )
        .drop("titleType")
        .join(ratings, "tconst", "left")
    )


def id_catalog(titles, people):
    return titles.select(
        F.col("tconst").alias("imdb_id"), F.lit("title").alias("entity_type")
    ).unionByName(
        people.select(
            F.col("nconst").alias("imdb_id"), F.lit("person").alias("entity_type")
        )
    )


def enrichment_candidates(titles, people, principals, selection):
    requested = selection.select(F.trim("tconst").alias("tconst")).distinct()
    parents = titles.select("tconst", "titleType", "startYear").withColumn(
        "_title_present", F.lit(True)
    )
    selections = (
        requested.join(parents, "tconst", "left")
        .withColumn(
            "selection_status",
            F.when(
                F.col("tconst").isNull() | ~F.col("tconst").rlike(r"^tt[0-9]{7,}$"),
                "invalid_identifier",
            )
            .when(F.col("_title_present").isNull(), "not_in_prepared_titles")
            .when(~F.col("titleType").eqNullSafe("movie"), "not_movie")
            .when(
                F.col("startYear").isNull() | ~F.col("startYear").between(1980, 2025),
                "missing_or_outside_year_range",
            )
            .otherwise("selected"),
        )
        .select("tconst", "selection_status")
    )
    chosen = selections.filter("selection_status = 'selected'").select("tconst")
    credits = principals.join(chosen, "tconst", "inner")
    accepted = credits.join(people, "nconst", "left_semi")
    candidates = accepted.select("tconst", "nconst").distinct()
    rejected = (
        credits.join(people, "nconst", "left_anti")
        .withColumn(
            "reason",
            F.when(F.col("nconst").isNull(), "missing_person_identifier").otherwise(
                "person_not_in_prepared_names"
            ),
        )
        .select("tconst", "ordering", "nconst", "category", "reason")
    )
    counts = credits.groupBy("tconst").agg(F.count("*").alias("principal_rows"))
    available = candidates.groupBy("tconst").agg(F.count("*").alias("candidate_people"))
    rejected_counts = rejected.groupBy("tconst").agg(
        F.count("*").alias("rejected_credit_rows")
    )
    coverage = (
        selections.join(counts, "tconst", "left")
        .join(available, "tconst", "left")
        .join(rejected_counts, "tconst", "left")
        .fillna(0, ["principal_rows", "candidate_people", "rejected_credit_rows"])
        .withColumn(
            "fewer_than_three_candidates",
            (F.col("selection_status") == "selected") & (F.col("candidate_people") < 3),
        )
    )
    return candidates, coverage, rejected, accepted


def filter_reference(reference, ids):
    normalized = reference.select(*[F.trim(c).alias(c) for c in REFERENCE_COLUMNS])
    valid = (
        F.col("wiki").rlike(r"^[a-z0-9_-]+wiki$")
        & (F.length("title") > 0)
        & F.col("imdb_id").rlike(r"^(?:tt|nm)[0-9]{7,}$")
        & (
            F.col("wikidata_id").isNull()
            | (F.col("wikidata_id") == "")
            | F.col("wikidata_id").rlike(r"^Q[0-9]+$")
        )
    )
    tagged = normalized.withColumn("_valid", F.coalesce(valid, F.lit(False)))
    malformed = (
        tagged.filter(~F.col("_valid"))
        .drop("_valid")
        .withColumn("reason", F.lit("invalid_reference"))
    )
    usable = tagged.filter("_valid").drop("_valid")
    known = ids.select("imdb_id")
    absent = (
        usable.join(known, "imdb_id", "left_anti")
        .select(*REFERENCE_COLUMNS)
        .withColumn("reason", F.lit("identifier_not_in_prepared_snapshot"))
    )
    matched = usable.join(known, "imdb_id", "left_semi").select(*REFERENCE_COLUMNS)
    return matched.distinct(), malformed.unionByName(absent), matched


def read_csv(spark, path, columns):
    return (
        spark.read.schema(", ".join(c + " string" for c in columns))
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


def single_csv(frame, path):
    """Materialize a real UTF-8 file without collecting source rows in Python."""
    path = Path(path)
    work = path.with_name("_" + path.name + "_parts")
    (
        frame.coalesce(1)
        .sortWithinPartitions(*frame.columns)
        .write.mode("errorifexists")
        .option("header", True)
        .option("escape", '"')
        .csv(str(work))
    )
    parts = list(work.glob("part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one CSV part in {work}")
    os.replace(parts[0], path)
    shutil.rmtree(work)


def export(
    spark,
    prepared_dir,
    output,
    selected_titles=None,
    wiki_reference=None,
    reference_source=None,
):
    prepared_dir, output = Path(prepared_dir).resolve(), Path(output).resolve()
    if (
        output == prepared_dir
        or prepared_dir in output.parents
        or output in prepared_dir.parents
    ):
        raise ValueError("Prepared input and export output must be separate")
    if output.exists():
        raise FileExistsError(f"Export output already exists: {output}")
    frames, prepared_summary = load_prepared(spark, prepared_dir)
    inputs = {"prepared_summary_sha256": sha256(prepared_dir / "summary.json")}
    for name, path in (
        ("selected_titles", selected_titles),
        ("wiki_reference", wiki_reference),
    ):
        if path is not None:
            inputs[name] = {"path": str(Path(path).resolve()), "sha256": sha256(path)}
    if reference_source is not None:
        if wiki_reference is None:
            raise ValueError("--reference-source requires --wiki-reference")
        inputs["wiki_reference"]["source_uri"] = reference_source
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    staging.mkdir()
    print(f"Staging exports: {staging}", flush=True)
    summary = {
        "scope": prepared_summary["scope"],
        "spark_version": spark.version,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "inputs": inputs,
        "movie_catalog_definition": "IMDb titleType=movie, startYear 1980..2025; no runtime threshold or ranking",
        "selection_method": "external_tconst_list"
        if selected_titles
        else "not_requested",
    }
    cached = []

    def persist(frame):
        frame = frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        return frame

    try:
        ids = persist(id_catalog(frames["title.basics"], frames["name.basics"]))
        ids.write.mode("errorifexists").parquet(str(staging / "imdb_ids"))
        catalog = persist(
            movie_catalog(frames["title.basics"], frames["title.ratings"])
        )
        catalog.write.mode("errorifexists").parquet(str(staging / "movie_catalog"))
        summary["imdb_identifiers"] = ids.count()
        summary["movies_1980_2025"] = catalog.count()
        if selected_titles:
            selection = persist(read_csv(spark, selected_titles, ["tconst"]))
            candidates, coverage, rejected, accepted = enrichment_candidates(
                frames["title.basics"],
                frames["name.basics"],
                frames["title.principals"],
                selection,
            )
            candidates, coverage, rejected = map(
                persist, (candidates, coverage, rejected)
            )
            single_csv(candidates, staging / "candidates.csv")
            coverage.write.mode("errorifexists").parquet(
                str(staging / "enrichment_coverage")
            )
            rejected.write.mode("errorifexists").parquet(
                str(staging / "rejected_credits")
            )
            candidate_count = candidates.count()
            duplicate_count = accepted.count() - candidate_count
            rejected_count = rejected.count()
            principal_count = coverage.agg(F.sum("principal_rows")).first()[0] or 0
            if principal_count != candidate_count + duplicate_count + rejected_count:
                raise AssertionError("Credit row reconciliation failed")
            requested_count = selection.count()
            distinct_titles = coverage.count()
            summary["enrichment"] = {
                "requested_rows": requested_count,
                "distinct_requested_titles": distinct_titles,
                "duplicate_selection_rows_collapsed": requested_count - distinct_titles,
                "status_counts": {
                    r.selection_status: r["count"]
                    for r in coverage.groupBy("selection_status").count().collect()
                },
                "candidate_pairs": candidate_count,
                "principal_rows": principal_count,
                "duplicate_credit_pairs_collapsed": duplicate_count,
                "rejected_credit_rows": rejected_count,
                "films_with_fewer_than_three_candidates": coverage.filter(
                    "fewer_than_three_candidates"
                ).count(),
                "consumer_ready": candidate_count > 0,
            }
        if wiki_reference:
            reference = persist(read_csv(spark, wiki_reference, REFERENCE_COLUMNS))
            matched, rejected, before_dedup = filter_reference(reference, ids)
            matched, rejected = map(persist, (matched, rejected))
            single_csv(matched, staging / "streaming_reference.csv")
            rejected.write.mode("errorifexists").parquet(
                str(staging / "rejected_reference")
            )
            matched_count = matched.count()
            duplicate_count = before_dedup.count() - matched_count
            rejected_count = rejected.count()
            input_count = reference.count()
            if input_count != matched_count + duplicate_count + rejected_count:
                raise AssertionError("Reference row reconciliation failed")
            summary["streaming_reference"] = {
                "input_rows": input_count,
                "matched_rows": matched_count,
                "duplicate_rows_collapsed": duplicate_count,
                "rejected_rows": rejected_count,
                "rejection_counts": {
                    r.reason: r["count"]
                    for r in rejected.groupBy("reason").count().collect()
                },
                "absent_identifiers_are_unverified": prepared_summary["scope"]
                == "sample",
                "consumer_ready": matched_count > 0,
            }
        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        json_write(staging / "summary.json", summary)
        json_write(
            staging / "SUCCESS.json",
            {"scope": summary["scope"], "completed_at": summary["completed_at"]},
        )
        os.rename(staging, output)
        return summary
    finally:
        for frame in cached:
            frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--selected-titles", type=Path, help="CSV with exactly one column, tconst"
    )
    parser.add_argument(
        "--wiki-reference",
        type=Path,
        help="Existing wiki,title,imdb_id,wikidata_id CSV",
    )
    parser.add_argument(
        "--reference-source",
        help="Source URI of the supplied Wikipedia reference snapshot",
    )
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if args.reference_source and not args.wiki_reference:
        parser.error("--reference-source requires --wiki-reference")
    spark = (
        SparkSession.builder.master(args.master)
        .appName("IMDb integration exports")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = export(
            spark,
            args.prepared_dir,
            args.output,
            args.selected_titles,
            args.wiki_reference,
            args.reference_source,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
