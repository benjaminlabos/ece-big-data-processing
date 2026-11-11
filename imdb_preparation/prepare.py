"""Prepare all seven tables: python -m imdb_preparation.prepare --help."""

import argparse
import json
import os
import platform
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from .download import sha256
from .quality import (
    check_reference,
    clean_values,
    normalize,
    partition_keys,
    profile,
)
from .schema import REFERENCES, TABLES, raw_schema


def json_write(path, data):
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def inspect_snapshot(raw_dir):
    raw_dir = Path(raw_dir).resolve()
    manifest = json.loads((raw_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("scope") not in ("full", "sample", "fixture"):
        raise ValueError("manifest.scope must be full, sample or fixture")
    if set(manifest.get("files", {})) != set(TABLES):
        raise ValueError("Snapshot must contain exactly the seven IMDb tables")
    paths = {}
    for table, metadata in manifest["files"].items():
        path = (raw_dir / metadata["path"]).resolve()
        if path.parent != raw_dir or not path.is_file():
            raise ValueError(f"Invalid local input path for {table}")
        if sha256(path) != metadata["sha256"]:
            raise ValueError(f"SHA256 mismatch for {table}")
        paths[table] = path
    return manifest, paths


def prepare(spark, raw_dir, output, as_of_year):
    raw_dir, output = Path(raw_dir).resolve(), Path(output).resolve()
    if output == raw_dir or raw_dir in output.parents or output in raw_dir.parents:
        raise ValueError("Input and output directories must be separate")
    if output.exists():
        raise FileExistsError(
            f"Output already exists: {output}; choose a new run directory"
        )
    manifest, paths = inspect_snapshot(raw_dir)
    repair = manifest["scope"] != "sample"
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    staging.mkdir()
    print(f"Staging results: {staging}", flush=True)
    frames = {}
    profiles = []
    metrics = {}
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        # Both parent tables precede dependent tables; name->title is resolved here.
        for table in TABLES:
            print(f"Preparing {table}", flush=True)
            raw = (
                spark.read.schema(raw_schema(table))
                .option("header", True)
                .option("sep", "\t")
                .option("quote", "")
                .option("escape", "")
                .option("mode", "FAILFAST")
                .option("enforceSchema", False)
                .option("encoding", "UTF-8")
                .csv(str(paths[table]))
            )
            raw_count, entries = profile(table, raw)
            profiles.extend(entries)
            distinct, valid, rejected = partition_keys(table, normalize(raw))
            distinct = distinct.persist(StorageLevel.DISK_ONLY)
            distinct_count = distinct.count()
            rejected.write.mode("errorifexists").parquet(
                str(staging / "quarantine" / table / "keys")
            )
            valid = valid.persist(StorageLevel.DISK_ONLY)
            valid_count = valid.count()
            distinct.unpersist(blocking=True)
            typed, events = clean_values(table, valid, as_of_year)
            audit_dir = staging / "anomalies" / table
            events.write.mode("errorifexists").parquet(str(audit_dir / "values"))
            current = typed.persist(StorageLevel.DISK_ONLY)
            current.count()
            valid.unpersist(blocking=True)
            for column, parent_table, parent_key, is_array in REFERENCES.get(table, []):
                following, events, rejected = check_reference(
                    table,
                    current,
                    column,
                    frames[parent_table],
                    parent_key,
                    is_array,
                    repair,
                )
                events.write.mode("errorifexists").parquet(str(audit_dir / column))
                rejected.write.mode("errorifexists").parquet(
                    str(staging / "quarantine" / table / column)
                )
                if repair:
                    following = following.persist(StorageLevel.DISK_ONLY)
                    following.count()
                    current.unpersist(blocking=True)
                    current = following
            clean_dir = staging / "clean" / table
            current.write.mode("errorifexists").parquet(str(clean_dir))
            frames[table] = spark.read.parquet(str(clean_dir))
            clean_count = frames[table].count()
            audit = spark.read.option("recursiveFileLookup", True).parquet(
                str(audit_dir)
            )
            quarantine_count = (
                spark.read.option("recursiveFileLookup", True)
                .parquet(str(staging / "quarantine" / table))
                .count()
            )
            by_rule = [
                r.asDict()
                for r in audit.groupBy("column", "rule", "action")
                .count()
                .orderBy("column", "rule")
                .collect()
            ]
            metrics[table] = {
                "raw_rows": raw_count,
                "normalized_duplicate_rows_removed": raw_count - distinct_count,
                "invalid_or_conflicting_key_rows": distinct_count - valid_count,
                "quarantined_rows": quarantine_count,
                "clean_rows": clean_count,
                "anomalies": by_rule,
            }
            removed_duplicates = raw_count - distinct_count
            if raw_count != clean_count + quarantine_count + removed_duplicates:
                raise AssertionError(f"Row reconciliation failed for {table}")
            current.unpersist(blocking=True)
            json_write(
                staging / "progress.tmp",
                {
                    "scope": manifest["scope"],
                    "completed_tables": list(metrics),
                    "tables": metrics,
                },
            )
            os.replace(staging / "progress.tmp", staging / "progress.json")
            print(
                f"{table}: {raw_count} raw -> {clean_count} clean, {quarantine_count} quarantined",
                flush=True,
            )

        json_write(staging / "missing_profile.json", profiles)
        ranked = sorted(
            [entry for entry in profiles if entry["missing_percent"] is not None],
            key=lambda entry: (
                -entry["missing_percent"],
                entry["table"],
                entry["column"],
            ),
        )
        json_write(staging / "top5_missing.json", ranked[:5])
        summary = {
            "scope": manifest["scope"],
            "reference_policy": "repair_against_clean_parents"
            if repair
            else "report_only_sample_parents_incomplete",
            "as_of_year": as_of_year,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "python_version": platform.python_version(),
            "java_version": spark.sparkContext._jvm.java.lang.System.getProperty(
                "java.version"
            ),
            "tables": metrics,
        }
        json_write(staging / "summary.json", summary)
        json_write(staging / "input_manifest.json", manifest)
        json_write(
            staging / "SUCCESS.json",
            {"scope": manifest["scope"], "completed_at": summary["completed_at"]},
        )
        # The final directory becomes visible only once every table has succeeded.
        os.rename(staging, output)
        return summary
    finally:
        spark.catalog.clearCache()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--as-of-year", type=int, default=date.today().year)
    args = parser.parse_args()
    if not 1 <= args.as_of_year <= 9999 or args.shuffle_partitions < 1:
        parser.error("Invalid --as-of-year or --shuffle-partitions")
    # Master is explicit; the default bounds local concurrency to two threads.
    spark = (
        SparkSession.builder.master(args.master)
        .appName("IMDb data preparation")
        .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = prepare(spark, args.raw_dir, args.output, args.as_of_year)
        print(
            json.dumps(
                {
                    "output": str(args.output.resolve()),
                    "scope": summary["scope"],
                    "tables": len(summary["tables"]),
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
