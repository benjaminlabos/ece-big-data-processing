"""Export verified credits for a selected title subset without cleaning unrelated credits."""

import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from .download import sha256
from .exports import enrichment_candidates, read_csv, single_csv
from .prepare import json_write
from .quality import (
    check_reference,
    clean_values,
    missing,
    normalize,
    partition_keys,
    trim_whitespace,
)
from .schema import raw_schema


def select_credit_rows(raw, chosen):
    key = F.when(missing("tconst"), F.lit(None).cast("string")).otherwise(
        trim_whitespace("tconst")
    )
    return raw.withColumn("tconst", key).join(
        F.broadcast(chosen.select("tconst").distinct()), "tconst", "left_semi"
    )


def run(spark, prepared, snapshot, raw_profile, selection_path, output):
    prepared, snapshot, raw_profile, selection_path, output = map(
        lambda p: Path(p).resolve(),
        (prepared, snapshot, raw_profile, selection_path, output),
    )
    if output.exists():
        raise FileExistsError(output)
    for source in (prepared, snapshot, raw_profile, selection_path):
        if output == source or source in output.parents or output in source.parents:
            raise ValueError("Separate output required")
    progress_bytes = (prepared / "progress.json").read_bytes()
    progress = json.loads(progress_bytes)
    profile = json.loads((raw_profile / "summary.json").read_text())
    manifest = json.loads((snapshot / "manifest.json").read_text())
    if not (raw_profile / "PROFILE_SUCCESS.json").exists() or profile[
        "manifest_sha256"
    ] != sha256(snapshot / "manifest.json"):
        raise ValueError("Verified raw snapshot profile required")
    if (
        profile["scope"] != progress["scope"]
        or progress["scope"] != manifest["scope"]
        or profile["scope"] == "sample"
    ):
        raise ValueError("Source scopes differ or are a sample")
    for table in ("title.basics", "name.basics"):
        if (
            table not in progress["completed_tables"]
            or progress["tables"][table]["raw_rows"] != profile["rows"][table]
        ):
            raise ValueError("Completed title and name tables required")
    path = (snapshot / manifest["files"]["title.principals"]["path"]).resolve()
    if (
        path.parent != snapshot
        or sha256(path) != manifest["files"]["title.principals"]["sha256"]
    ):
        raise ValueError("Principal source checksum mismatch")
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    cached = []

    def cache(frame):
        cached.append(frame.persist(StorageLevel.DISK_ONLY))
        return cached[-1]

    try:
        selection = cache(normalize(read_csv(spark, selection_path, ["tconst"])))
        titles = spark.read.parquet(str(prepared / "clean" / "title.basics")).select(
            "tconst", "titleType", "startYear"
        )
        people = spark.read.parquet(str(prepared / "clean" / "name.basics")).select(
            "nconst"
        )
        for table, frame in (("title.basics", titles), ("name.basics", people)):
            if frame.count() != progress["tables"][table]["clean_rows"]:
                raise ValueError("Parent checkpoint row count differs")
        title_subset = cache(
            titles.join(
                F.broadcast(selection.select("tconst").distinct()),
                "tconst",
                "left_semi",
            )
        )
        chosen = cache(
            title_subset.filter(
                (F.col("titleType") == "movie") & F.col("startYear").between(1980, 2025)
            ).select("tconst")
        )
        raw = (
            spark.read.schema(raw_schema("title.principals"))
            .option("header", True)
            .option("sep", "\t")
            .option("quote", "")
            .option("escape", "")
            .option("mode", "FAILFAST")
            .option("enforceSchema", False)
            .csv(str(path))
        )
        subset = cache(select_credit_rows(raw, chosen))
        raw_count = subset.count()
        print(f"Selected title credit rows: {raw_count}", flush=True)
        distinct, valid, keys = partition_keys("title.principals", normalize(subset))
        distinct, valid = cache(distinct), cache(valid)
        distinct_count, valid_count = distinct.count(), valid.count()
        keys.write.parquet(str(stage / "key_quarantine"))
        typed, scalar_events = clean_values("title.principals", valid, 2026)
        typed = cache(typed)
        person_subset = cache(
            people.join(
                F.broadcast(typed.select("nconst").distinct()), "nconst", "left_semi"
            )
        )
        cleaned, reference_events, _ = check_reference(
            "title.principals", typed, "nconst", person_subset, "nconst", False, True
        )
        cleaned = cache(cleaned)
        scalar_events.unionByName(reference_events).write.parquet(
            str(stage / "anomalies")
        )
        cleaned.write.parquet(str(stage / "clean_selected_principals"))
        if cleaned.count() != valid_count:
            raise AssertionError("Selected credit cleaning row reconciliation failed")
        candidates, coverage, rejected, accepted = enrichment_candidates(
            title_subset, person_subset, cleaned, selection
        )
        candidates, coverage, rejected, accepted = map(
            cache, (candidates, coverage, rejected, accepted)
        )
        pairs, accepted_count, rejected_count = (
            candidates.count(),
            accepted.count(),
            rejected.count(),
        )
        principal_rows = coverage.agg(F.sum("principal_rows")).first()[0] or 0
        if (
            principal_rows != pairs + (accepted_count - pairs) + rejected_count
            or principal_rows != valid_count
        ):
            raise AssertionError("Candidate credit reconciliation failed")
        single_csv(candidates, stage / "candidates.csv")
        single_csv(
            accepted.select("tconst", "nconst", "ordering", "category"),
            stage / "credit_details.csv",
        )
        single_csv(coverage, stage / "coverage.csv")
        rejected.write.parquet(str(stage / "rejected_credits"))
        summary = {
            "scope": profile["scope"] + "_selected_credits_subset",
            "complete_preparation_validated": False,
            "student_validated": False,
            "spark_version": spark.version,
            "source_manifest_sha256": profile["manifest_sha256"],
            "selected_titles_sha256": sha256(selection_path),
            "source_progress_sha256": hashlib.sha256(progress_bytes).hexdigest(),
            "module_sha256": sha256(Path(__file__)),
            "source_principal_rows": profile["rows"]["title.principals"],
            "selected_raw_credit_rows": raw_count,
            "duplicates_removed": raw_count - distinct_count,
            "key_quarantined_rows": distinct_count - valid_count,
            "requested_rows": selection.count(),
            "distinct_requested_titles": coverage.count(),
            "status_counts": {
                r.selection_status: r["count"]
                for r in coverage.groupBy("selection_status").count().collect()
            },
            "candidate_pairs": pairs,
            "accepted_credit_rows": accepted_count,
            "duplicate_credit_pairs_collapsed": accepted_count - pairs,
            "rejected_credit_rows": rejected_count,
            "films_with_fewer_than_three_candidates": coverage.filter(
                "fewer_than_three_candidates"
            ).count(),
            "consumer_ready": pairs > 0,
            "title_reference_policy": "selected subset inner-joined to completed clean movie parents before source filtering",
        }
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
    for name in ("prepared", "snapshot", "raw-profile", "selected-titles", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    a = p.parse_args()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Selected film credit export")
        .config("spark.sql.shuffle.partitions", 16)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(
            json.dumps(
                run(
                    spark,
                    a.prepared,
                    a.snapshot,
                    a.raw_profile,
                    a.selected_titles,
                    a.output,
                ),
                indent=2,
            )
        )
    finally:
        spark.stop()
