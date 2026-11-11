"""Prepare all seven tables: python -m imdb_preparation.prepare --help."""

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
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
from .schema import ARRAYS, INTEGERS, REFERENCES, TABLES, raw_schema


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


def canonical_source_sha(path):
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def load_raw_profile(directory, raw_dir, scope):
    directory = Path(directory).resolve()
    if not (directory / "PROFILE_SUCCESS.json").is_file():
        raise ValueError("Completed raw profile required")
    summary = json.loads((directory / "summary.json").read_text())
    marker = json.loads((directory / "PROFILE_SUCCESS.json").read_text())
    if marker != summary or summary.get("kind") != "raw_missingness_only":
        raise ValueError("Raw profile marker and summary differ")
    if summary["scope"] != scope or summary["manifest_sha256"] != sha256(
        raw_dir / "manifest.json"
    ):
        raise ValueError("Raw profile snapshot mismatch")
    entries = json.loads((directory / "missing_profile.json").read_text())
    expected = {
        (table, column) for table, columns in TABLES.items() for column in columns
    }
    if (
        len(entries) != len(expected)
        or {(e["table"], e["column"]) for e in entries} != expected
    ):
        raise ValueError("Raw profile columns incomplete or duplicated")
    grouped = {table: [] for table in TABLES}
    for entry in entries:
        rows, missing_rows = entry["total_rows"], entry["missing_rows"]
        if rows != summary["rows"][entry["table"]] or not 0 <= missing_rows <= rows:
            raise ValueError("Raw profile row counts inconsistent")
        percentage = entry["missing_percent"]
        if (rows == 0 and percentage is not None) or (
            rows > 0
            and (
                percentage is None
                or not math.isclose(percentage, 100.0 * missing_rows / rows)
            )
        ):
            raise ValueError("Raw profile percentages inconsistent")
        grouped[entry["table"]].append(entry)
    for table in TABLES:
        grouped[table].sort(key=lambda e: TABLES[table].index(e["column"]))
    return grouped


def adopt_checkpoint(spark, source, staging, context, precomputed, legacy_year):
    """Reuse completed immutable tables; never adopt an unfinished table."""
    source = Path(source).resolve()
    progress = json.loads((source / "progress.json").read_text())
    completed = progress["completed_tables"]
    if (
        not completed
        or completed != list(TABLES)[: len(completed)]
        or set(progress["tables"]) != set(completed)
    ):
        raise ValueError("Checkpoint must contain a completed table prefix")
    if progress["scope"] != context["scope"]:
        raise ValueError("Checkpoint scope mismatch")
    previous_context = source / "checkpoint_context.json"
    legacy = not previous_context.exists()
    if legacy:
        if legacy_year != context["as_of_year"]:
            raise ValueError(
                "Legacy checkpoint requires its original as-of year explicitly"
            )
    else:
        previous = json.loads(previous_context.read_text())
        if any(
            previous.get(key) != context[key]
            for key in (
                "scope",
                "as_of_year",
                "input_manifest_sha256",
                "quality_source_sha256",
                "schema_source_sha256",
            )
        ):
            raise ValueError("Checkpoint inputs or cleaning rules differ")
    frames, metrics = {}, {}
    for table in completed:
        item = progress["tables"][table]
        if item["raw_rows"] != precomputed[table][0]["total_rows"]:
            raise ValueError("Checkpoint and raw profile row counts differ")
        leaves = {
            "clean": [""],
            "anomalies": ["values"] + [r[0] for r in REFERENCES.get(table, [])],
            "quarantine": ["keys"] + [r[0] for r in REFERENCES.get(table, [])],
        }
        for kind, names in leaves.items():
            for name in names:
                if not (source / kind / table / name / "_SUCCESS").is_file():
                    raise ValueError(f"Incomplete checkpoint: {kind}/{table}/{name}")
        clean = spark.read.parquet(str(source / "clean" / table))
        expected_types = {c: "bigint" for c in INTEGERS[table]}
        expected_types.update({c: "array<string>" for c in ARRAYS.get(table, {})})
        if table == "title.ratings":
            expected_types["averageRating"] = "double"
        if set(clean.columns) != set(TABLES[table]) or any(
            field.dataType.simpleString() != expected_types.get(field.name, "string")
            for field in clean.schema
        ):
            raise ValueError("Checkpoint schema mismatch")
        clean_count = clean.count()
        quarantined = (
            spark.read.option("recursiveFileLookup", True)
            .parquet(str(source / "quarantine" / table))
            .count()
        )
        audit = spark.read.option("recursiveFileLookup", True).parquet(
            str(source / "anomalies" / table)
        )
        rules = [
            row.asDict()
            for row in audit.groupBy("column", "rule", "action").count().collect()
        ]

        def by_rule(row):
            return row["column"], row["rule"], row["action"]

        if (
            clean_count != item["clean_rows"]
            or quarantined != item["quarantined_rows"]
            or sorted(rules, key=by_rule) != sorted(item["anomalies"], key=by_rule)
        ):
            raise ValueError("Checkpoint counts or audit differ")
        if (
            item["raw_rows"]
            != clean_count + quarantined + item["normalized_duplicate_rows_removed"]
        ):
            raise ValueError("Checkpoint row reconciliation failed")
        for kind in leaves:
            root = source / kind / table
            if any(path.is_symlink() for path in root.rglob("*")):
                raise ValueError("Checkpoint symlinks are not supported")
            shutil.copytree(root, staging / kind / table, copy_function=os.link)
        frames[table] = spark.read.parquet(str(staging / "clean" / table))
        metrics[table] = item
        print(f"Reused completed {table}: {clean_count} clean rows", flush=True)
    return (
        frames,
        metrics,
        {
            "source": str(source),
            "reused_tables": completed,
            "legacy_context_asserted": legacy,
            "legacy_original_year": legacy_year if legacy else None,
            "source_progress_sha256": sha256(source / "progress.json"),
            "copy_method": "hardlink",
            "checks": "source profile, scope, year, schema, success markers, clean/quarantine/audit counts",
        },
    )


def prepare(
    spark,
    raw_dir,
    output,
    as_of_year,
    resume_from=None,
    raw_profile=None,
    legacy_as_of_year=None,
    input_partitions=None,
    stop_after=None,
):
    raw_dir, output = Path(raw_dir).resolve(), Path(output).resolve()
    if output == raw_dir or raw_dir in output.parents or output in raw_dir.parents:
        raise ValueError("Input and output directories must be separate")
    if output.exists():
        raise FileExistsError(
            f"Output already exists: {output}; choose a new run directory"
        )
    manifest, paths = inspect_snapshot(raw_dir)
    if input_partitions is not None and input_partitions < 1:
        raise ValueError("Input partitions must be positive")
    if stop_after is not None and stop_after not in TABLES:
        raise ValueError("Unknown checkpoint stop table")
    if resume_from and not raw_profile:
        raise ValueError("Recovery requires a completed raw profile")
    if resume_from:
        resume_path = Path(resume_from).resolve()
        if (
            output == resume_path
            or resume_path in output.parents
            or output in resume_path.parents
        ):
            raise ValueError("Recovery output must be separate")
    precomputed = (
        load_raw_profile(raw_profile, raw_dir, manifest["scope"])
        if raw_profile
        else None
    )
    repair = manifest["scope"] != "sample"
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    staging.mkdir()
    context = {
        "scope": manifest["scope"],
        "as_of_year": as_of_year,
        "input_manifest_sha256": sha256(raw_dir / "manifest.json"),
        "quality_source_sha256": canonical_source_sha(
            Path(__file__).with_name("quality.py")
        ),
        "schema_source_sha256": canonical_source_sha(
            Path(__file__).with_name("schema.py")
        ),
    }
    json_write(staging / "checkpoint_context.json", context)
    json_write(staging / "input_manifest.json", manifest)
    print(f"Staging results: {staging}", flush=True)
    frames = {}
    profiles = []
    metrics = {}
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        recovery = None
        if resume_from:
            frames, metrics, recovery = adopt_checkpoint(
                spark, resume_from, staging, context, precomputed, legacy_as_of_year
            )
            if stop_after and list(TABLES).index(stop_after) < len(metrics):
                raise ValueError("Checkpoint stop table must follow resumed tables")
            for table in metrics:
                profiles.extend(precomputed[table])
            json_write(
                staging / "progress.json",
                {
                    "scope": manifest["scope"],
                    "completed_tables": list(metrics),
                    "tables": metrics,
                },
            )
        # Both parent tables precede dependent tables; name->title is resolved here.
        for table in TABLES:
            if table in metrics:
                continue
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
            if precomputed:
                entries = precomputed[table]
                raw_count = entries[0]["total_rows"]
            else:
                raw_count, entries = profile(table, raw)
            profiles.extend(entries)
            # Gzip is unsplittable. Shuffle raw fields before expensive Unicode
            # normalization, so that it and partial dedup can use all workers.
            if input_partitions:
                raw = raw.repartition(input_partitions)
            distinct, valid, rejected = partition_keys(table, normalize(raw))
            distinct = distinct.persist(StorageLevel.DISK_ONLY)
            distinct_count = distinct.count()
            if distinct_count > raw_count:
                raise ValueError("Distinct rows exceed the verified raw row count")
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
            if table == stop_after:
                break

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
            "recovery": recovery,
            "precomputed_raw_profile": str(Path(raw_profile).resolve())
            if raw_profile
            else None,
            "input_partitions": input_partitions,
            "preparation_module_sha256": sha256(Path(__file__)),
            "checkpoint_only": len(metrics) != len(TABLES),
        }
        json_write(staging / "summary.json", summary)
        json_write(staging / "input_manifest.json", manifest)
        json_write(
            staging
            / ("CHECKPOINT.json" if summary["checkpoint_only"] else "SUCCESS.json"),
            {
                "scope": manifest["scope"],
                "completed_at": summary["completed_at"],
                "checkpoint_only": summary["checkpoint_only"],
                "completed_tables": list(metrics),
            },
        )
        # Partial prefixes have CHECKPOINT only; consumers still require SUCCESS.
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
    parser.add_argument("--resume-from", type=Path)
    parser.add_argument("--raw-profile", type=Path)
    parser.add_argument("--legacy-as-of-year", type=int)
    parser.add_argument("--input-partitions", type=int)
    parser.add_argument("--stop-after", choices=TABLES)
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
        summary = prepare(
            spark,
            args.raw_dir,
            args.output,
            args.as_of_year,
            args.resume_from,
            args.raw_profile,
            args.legacy_as_of_year,
            args.input_partitions,
            args.stop_after,
        )
        print(
            json.dumps(
                {
                    "output": str(args.output.resolve()),
                    "scope": summary["scope"],
                    "tables": len(summary["tables"]),
                    "checkpoint_only": summary["checkpoint_only"],
                }
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
