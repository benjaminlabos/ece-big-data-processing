"""Intersect the Wikimedia catalog with IMDb keys using the preparation code."""

import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, functions as F

from imdb_preparation.download import sha256
from imdb_preparation.exports import REFERENCE_COLUMNS, filter_reference, read_csv, single_csv
from imdb_preparation.quality import normalize, partition_keys
from imdb_preparation.schema import raw_schema


PARENT_KEYS = {"title.basics": "tconst", "name.basics": "nconst"}


def source_fingerprints(path):
    if path.is_file():
        return {path.name: sha256(path)}
    parts = sorted(path.rglob("*.parquet"))
    if not parts:
        raise ValueError(f"No IMDb source files found at {path}")
    return {str(part.relative_to(path)): sha256(part) for part in parts}


def imdb_keys(spark, directory, wanted, raw):
    sources = {}
    keys = []
    for table, key in PARENT_KEYS.items():
        path = directory / (f"{table}.tsv.gz" if raw else f"clean/{table}")
        sources[table] = {"path": str(path), "sha256_by_file": source_fingerprints(path)}
        wanted_keys = F.broadcast(wanted.select(F.col("imdb_id").alias(key)))
        if raw:
            # All rows for each wanted key reach the same deduplication/quarantine rule.
            parent = (
                spark.read.schema(raw_schema(table)).option("header", True).option("sep", "\t")
                .option("quote", "").option("escape", "").option("mode", "FAILFAST")
                .option("enforceSchema", False).csv(str(path))
            )
            subset = normalize(parent).join(wanted_keys, key, "left_semi")
            _, parent, _ = partition_keys(table, subset)
        else:
            parent = spark.read.parquet(str(path)).select(key).join(wanted_keys, key, "left_semi")
        keys.append(parent.select(F.col(key).alias("imdb_id")))
    return keys[0].unionByName(keys[1]).distinct(), sources


def prepare_reference(spark, source, raw, catalog, output):
    if output.exists():
        raise FileExistsError(f"Reference output already exists: {output}")
    reference = read_csv(spark, catalog, REFERENCE_COLUMNS).cache()
    wanted = reference.select(F.trim("imdb_id").alias("imdb_id")).distinct()
    ids, sources = imdb_keys(spark, source, wanted, raw)
    ids = ids.cache()
    matched, rejected, before_dedup = filter_reference(reference, ids)
    matched, rejected = matched.cache(), rejected.cache()
    try:
        input_rows = reference.count()
        matched_rows = matched.count()
        rejected_counts = {row.reason: row["count"] for row in rejected.groupBy("reason").count().collect()}
        duplicates = before_dedup.count() - matched_rows
        if input_rows != matched_rows + sum(rejected_counts.values()) + duplicates:
            raise RuntimeError("Reference row reconciliation failed")
        if not matched_rows:
            raise ValueError("No IMDb key matched the Wikimedia catalog")
        counts = matched.agg(
            F.count_distinct(F.struct("wiki", "title")).alias("matched_pages"),
            F.count_distinct("imdb_id").alias("matched_imdb_ids"),
        ).first().asDict()
        output.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=output.parent, prefix=".reference-") as temporary:
            staged = Path(temporary) / "result"
            staged.mkdir()
            csv_path = staged / "streaming_reference.csv"
            single_csv(matched, csv_path)
            rejected.write.mode("errorifexists").parquet(str(staged / "rejected_reference"))
            summary = {
                "spark_version": spark.version,
                "input_kind": "raw_parent_keys_normalized_and_validated" if raw else "prepared_parent_parquet",
                "source": sources,
                "wikimedia_catalog_sha256": sha256(catalog),
                "streaming_reference_sha256": sha256(csv_path),
                "input_rows": input_rows,
                "matched_rows": matched_rows,
                **counts,
                "duplicate_rows_collapsed": duplicates,
                "rejected_counts": rejected_counts,
            }
            (staged / "summary.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            staged.rename(output)
        return summary
    finally:
        for frame in (matched, rejected, ids, reference):
            frame.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--imdb-prepared-dir", type=Path, help="Directory with clean/title.basics and clean/name.basics")
    source.add_argument("--imdb-raw-dir", type=Path, help="Directory with complete title.basics.tsv.gz and name.basics.tsv.gz")
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]").appName("PreparedImdbWikipediaReference")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = prepare_reference(
            spark, args.imdb_prepared_dir or args.imdb_raw_dir, bool(args.imdb_raw_dir),
            args.reference, args.output,
        )
        print(json.dumps(summary, ensure_ascii=False), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
