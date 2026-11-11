"""Inputs and evidence for the bounded GitHub-hosted IMDb batch."""

import argparse
import json
import shutil
from pathlib import Path

from .download import download, sha256
from .prepare import inspect_snapshot, json_write, load_raw_profile
from .schema import TABLES


def profile_inputs(raw, evidence, output):
    """Measure a new daily release instead of borrowing stale measurements."""
    manifest, _ = inspect_snapshot(raw)
    changed = [
        table
        for table in TABLES
        if manifest["files"][table]["sha256"]
        != evidence["input_manifest"]["files"][table]["sha256"]
    ]
    if not changed:
        seed_profile(raw, evidence, output)
        return
    from pyspark.sql import SparkSession
    from .profile_full import run

    print(
        f"New IMDb release ({', '.join(changed)}); measuring all 39 columns anew",
        flush=True,
    )
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("Cloud current snapshot profile")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run(spark, raw, output, workers=4)
        summary["profile_origin"] = "independent_new_snapshot_measurement"
        summary["changed_tables_from_previous_snapshot"] = changed
        json_write(Path(output) / "summary.json", summary)
        json_write(Path(output) / "PROFILE_SUCCESS.json", summary)
    finally:
        spark.stop()


def seed_profile(raw, evidence, output):
    """Reuse measured missingness only when all seven source hashes match."""
    raw, output = Path(raw), Path(output)
    manifest, _ = inspect_snapshot(raw)
    expected = evidence["input_manifest"]
    if manifest["scope"] != "full" or expected["scope"] != "full":
        raise ValueError("Full source snapshots required")
    for table in TABLES:
        if manifest["files"][table]["sha256"] != expected["files"][table]["sha256"]:
            raise ValueError(f"IMDb snapshot changed: {table}; refusing mixed results")
    output.mkdir(parents=True, exist_ok=False)
    summary = {
        **evidence["summary"],
        "manifest_sha256": sha256(raw / "manifest.json"),
        "reuse": "Existing measured profile; all seven source SHA256 hashes match",
        "original_manifest_sha256": evidence["summary"]["manifest_sha256"],
    }
    json_write(output / "summary.json", summary)
    json_write(output / "missing_profile.json", evidence["missing_profile"])
    json_write(output / "top5_missing.json", evidence["top5_missing"])
    json_write(output / "PROFILE_SUCCESS.json", summary)
    load_raw_profile(output, raw, "full")


def package(prepared, analysis, credits, destination):
    """Publish derived outputs only after checking every pipeline stage."""
    prepared, analysis, credits, destination = map(
        Path, (prepared, analysis, credits, destination)
    )
    summaries = []
    for directory in (prepared, analysis, credits):
        marker = json.loads((directory / "SUCCESS.json").read_text())
        summary = json.loads((directory / "summary.json").read_text())
        if marker.get("scope") != "full" or summary.get("scope") != "full":
            raise ValueError("Completed full-scope outputs required")
        summaries.append(summary)
    preparation, exploration, exports = summaries
    if set(preparation["tables"]) != set(TABLES):
        raise ValueError("Seven prepared tables required")
    for metrics in preparation["tables"].values():
        if metrics["raw_rows"] != sum(
            metrics[key]
            for key in (
                "clean_rows",
                "quarantined_rows",
                "normalized_duplicate_rows_removed",
            )
        ):
            raise ValueError("Prepared row reconciliation failed")
    expected_questions = {f"q{i}" for i in range(1, 9)} | {"top10_per_genre_year"}
    questions = exploration["questions"]
    if set(questions) != expected_questions or any(
        value["status"] not in ("computed", "provisional")
        for value in questions.values()
    ):
        raise ValueError("Eight completed questions and ranking required")
    if exploration["prepared_summary_sha256"] != sha256(prepared / "summary.json"):
        raise ValueError("Analysis preparation source mismatch")
    if not exports["enrichment"]["consumer_ready"]:
        raise ValueError("Usable enrichment input required")
    if len(json.loads((prepared / "missing_profile.json").read_text())) != 39:
        raise ValueError("39 column profiles required")
    profile_dir = Path(preparation["precomputed_raw_profile"])
    profile_summary = json.loads((profile_dir / "summary.json").read_text())
    if profile_summary["manifest_sha256"] != sha256(prepared / "input_manifest.json"):
        raise ValueError("Profile preparation source mismatch")
    destination.mkdir(parents=True, exist_ok=False)
    quality = destination / "quality"
    quality.mkdir()
    json_write(quality / "raw_profile_origin.json", profile_summary)
    for name in (
        "summary.json",
        "missing_profile.json",
        "top5_missing.json",
        "input_manifest.json",
        "checkpoint_context.json",
        "SUCCESS.json",
    ):
        shutil.copyfile(prepared / name, quality / name)
    shutil.copytree(analysis, destination / "analysis")
    enrichment = destination / "enrichment"
    enrichment.mkdir()
    for name in ("summary.json", "SUCCESS.json", "candidates.csv"):
        shutil.copyfile(credits / name, enrichment / name)
    for name in ("enrichment_coverage", "rejected_credits"):
        shutil.copytree(credits / name, enrichment / name)
    checksums = {
        str(path.relative_to(destination)): sha256(path)
        for path in sorted(destination.rglob("*"))
        if path.is_file()
    }
    json_write(
        destination / "VERIFIED.json",
        {
            "execution": "github_hosted_runner",
            "scope": "full",
            "student_validated": exploration["config"]["student_validated"],
            "questions": sorted(questions),
            "sha256": checksums,
            "note": "Raw profile bound to this input manifest; see quality/raw_profile_origin.json; cleaning recomputed",
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inputs", "package"))
    args = parser.parse_args()
    if args.action == "inputs":
        download(Path("data/raw/cloud-full"))
        evidence = json.loads(
            Path("validation/full-raw-profile-windows-2026-11-11.json").read_text()
        )
        profile_inputs(
            Path("data/raw/cloud-full"), evidence, Path("results/cloud-profile")
        )
    else:
        package(
            "results/cloud-prepared",
            "results/cloud-analysis",
            "results/cloud-credits",
            "artifacts/verified",
        )


if __name__ == "__main__":
    main()
