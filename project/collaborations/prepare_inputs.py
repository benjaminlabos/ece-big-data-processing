"""Prepare the analysis columns with the shared IMDb preparation functions."""

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from imdb_preparation.prepare import inspect_snapshot
from imdb_preparation.quality import check_reference, clean_values, normalize, partition_keys
from imdb_preparation.schema import raw_schema


def read_clean(spark, paths, table, as_of_year, scope=None):
    raw = (
        spark.read.schema(raw_schema(table))
        .option("header", True).option("sep", "\t").option("quote", "")
        .option("escape", "").option("mode", "FAILFAST").option("enforceSchema", False)
        .option("encoding", "UTF-8").csv(str(paths[table]))
    )
    normalized = normalize(raw)
    if scope is not None:
        ids, key = scope
        normalized = normalized.join(ids, key, "left_semi")
    _, valid, _ = partition_keys(table, normalized)
    cleaned, _ = clean_values(table, valid, as_of_year)
    return cleaned


def prepare(spark, raw_dir, output, selected, as_of_year):
    output = output.resolve()
    raw_dir = raw_dir.resolve()
    if output.exists():
        raise FileExistsError(f"Choose a new output directory: {output}")
    if output == raw_dir or output.is_relative_to(raw_dir):
        raise ValueError("Output must be outside the input directory")
    manifest, paths = inspect_snapshot(raw_dir)
    if manifest["scope"] != "full":
        raise ValueError("Complete IMDb source tables are required for the reference checks")
    selected_scope = None
    if selected:
        requested = (
            spark.read.schema("tconst STRING").option("header", True)
            .option("enforceSchema", False).option("mode", "FAILFAST").csv(str(selected))
        )
        if requested.where(~F.coalesce(F.col("tconst").rlike(r"^tt[0-9]{7,}$"), F.lit(False))).take(1):
            raise ValueError("Selected titles must contain valid tconst identifiers")
        selected_scope = (F.broadcast(requested.distinct()), "tconst")
    staging = output.with_name(f"{output.name}.incomplete-{uuid.uuid4().hex[:8]}")
    clean_dir = staging / "clean"
    clean_dir.mkdir(parents=True)
    counts = {}

    def publish(table, frame):
        path = str(clean_dir / table)
        frame.write.parquet(path)
        stored = spark.read.parquet(path)
        counts[table] = stored.count()
        print(f"Prepared {table}: {counts[table]} rows", flush=True)
        return stored

    titles = read_clean(spark, paths, "title.basics", as_of_year, selected_scope)
    titles = publish(
        "title.basics",
        titles.where(F.col("titleType") == "movie").select("tconst", "titleType", "startYear"),
    )
    title_ids = titles.select("tconst")
    title_scope = (F.broadcast(title_ids) if selected else title_ids, "tconst")
    crew = (
        read_clean(spark, paths, "title.crew", as_of_year, title_scope)
        .select("tconst", "directors").persist(StorageLevel.DISK_ONLY)
    )
    principals = (
        read_clean(spark, paths, "title.principals", as_of_year, title_scope)
        .where(F.col("category").isin("actor", "actress"))
        .select("tconst", "ordering", "nconst", "category").persist(StorageLevel.DISK_ONLY)
    )
    try:
        people = crew.select(F.explode("directors").alias("nconst")).unionByName(
            principals.select("nconst")
        ).distinct()
        print("Checking director and performer references", flush=True)
        names = read_clean(spark, paths, "name.basics", as_of_year, (people, "nconst"))
        names_path = str(staging / "people")
        names.select("nconst").write.parquet(names_path)
        names = spark.read.parquet(names_path)
        checked_crew, _, _ = check_reference(
            "title.crew", crew, "directors", names, "nconst", True, True
        )
        checked_principals, _, _ = check_reference(
            "title.principals", principals, "nconst", names, "nconst", False, True
        )
        publish("title.crew", checked_crew)
        publish("title.principals", checked_principals.select("tconst", "nconst", "category"))
        ratings = read_clean(spark, paths, "title.ratings", as_of_year, title_scope)
        publish("title.ratings", ratings.select("tconst", "averageRating", "numVotes"))
        receipt = {
            "scope": "sample" if selected else "full",
            "selected_titles_sha256": (
                hashlib.sha256(selected.read_bytes()).hexdigest() if selected else None
            ),
            "source_manifest_sha256": hashlib.sha256(
                (raw_dir / "manifest.json").read_bytes()
            ).hexdigest(),
            "sources": {
                table: manifest["files"][table]["sha256"]
                for table in ("title.basics", "title.crew", "title.principals", "title.ratings", "name.basics")
            },
            "as_of_year": as_of_year,
            "spark_version": spark.version,
            "projected_clean_rows": counts,
        }
        (staging / "input_manifest.json").write_text(
            json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
        )
        os.rename(staging, output)
    finally:
        crew.unpersist()
        principals.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--selected", type=Path, help="Optional CSV with one tconst column")
    parser.add_argument("--as-of-year", type=int, default=date.today().year)
    args = parser.parse_args()
    if not 1 <= args.as_of_year <= 9999:
        parser.error("--as-of-year must be in 1..9999")
    spark = SparkSession.builder.appName("IMDb collaboration inputs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        prepare(spark, args.raw_dir, args.output, args.selected, args.as_of_year)
    finally:
        spark.stop()
