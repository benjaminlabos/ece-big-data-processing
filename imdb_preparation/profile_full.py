"""Compute global raw missingness independently of the longer cleaning pipeline."""

import argparse
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from pyspark.sql import SparkSession

from .download import sha256
from .prepare import inspect_snapshot, json_write
from .quality import profile
from .schema import TABLES, raw_schema


def run(spark, raw_dir, output, workers=4):
    raw_dir, output = Path(raw_dir).resolve(), Path(output).resolve()
    if output.exists():
        raise FileExistsError(output)
    if output == raw_dir or raw_dir in output.parents or output in raw_dir.parents:
        raise ValueError("Separate output required")
    manifest, paths = inspect_snapshot(raw_dir)
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)

    def one(table):
        raw = (
            spark.read.schema(raw_schema(table))
            .option("header", True)
            .option("sep", "\t")
            .option("quote", "")
            .option("escape", "")
            .option("mode", "FAILFAST")
            .option("enforceSchema", False)
            .csv(str(paths[table]))
        )
        return table, profile(table, raw)

    completed = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, t) for t in TABLES]
        for future in as_completed(futures):
            table, (rows, entries) = future.result()
            completed[table] = {"rows": rows, "profile": entries}
            print(f"{table}: {rows} raw rows profiled", flush=True)
            json_write(
                stage / "progress.tmp",
                {
                    "scope": manifest["scope"],
                    "kind": "raw_missingness_only",
                    "completed_tables": list(completed),
                },
            )
            os.replace(stage / "progress.tmp", stage / "progress.json")
    entries = [entry for table in TABLES for entry in completed[table]["profile"]]
    ranked = sorted(
        [r for r in entries if r["missing_percent"] is not None],
        key=lambda r: (-r["missing_percent"], r["table"], r["column"]),
    )
    json_write(stage / "missing_profile.json", entries)
    json_write(stage / "top5_missing.json", ranked[:5])
    summary = {
        "scope": manifest["scope"],
        "kind": "raw_missingness_only",
        "manifest_sha256": sha256(raw_dir / "manifest.json"),
        "rows": {t: completed[t]["rows"] for t in TABLES},
        "cleaning_validated": False,
        "spark_version": spark.version,
    }
    json_write(stage / "summary.json", summary)
    json_write(stage / "PROFILE_SUCCESS.json", summary)
    os.rename(stage, output)
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw-dir", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--workers", type=int, default=4)
    a = p.parse_args()
    if not 1 <= a.workers <= 4:
        p.error("--workers must be 1..4")
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("IMDb complete raw profile")
        .config("spark.sql.shuffle.partitions", 2)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(run(spark, a.raw_dir, a.output, a.workers), indent=2))
    finally:
        spark.stop()
