import json

import pytest

from imdb_preparation.cloud import package, profile_inputs, seed_profile
from imdb_preparation.fixture import write_fixture
from imdb_preparation.profile_full import run


def test_cloud_profile_reuse_requires_identical_source_bytes(
    tmp_path, spark, monkeypatch
):
    raw = tmp_path / "raw"
    write_fixture(raw)
    manifest_path = raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    # Tiny controlled full-scope snapshot, not a claim about the IMDb full lot.
    manifest["scope"] = "full"
    manifest_path.write_text(json.dumps(manifest))
    original = tmp_path / "measured"
    summary = run(spark, raw, original, workers=2)
    evidence = {
        "input_manifest": manifest,
        "summary": summary,
        "missing_profile": json.loads((original / "missing_profile.json").read_text()),
        "top5_missing": json.loads((original / "top5_missing.json").read_text()),
    }
    seed_profile(raw, evidence, tmp_path / "reused")
    assert (
        json.loads((tmp_path / "reused/missing_profile.json").read_text())
        == evidence["missing_profile"]
    )
    evidence["input_manifest"]["files"]["title.principals"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="snapshot changed"):
        seed_profile(raw, evidence, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()
    expected_rows = dict(summary["rows"])
    evidence["summary"]["rows"] = {}  # Deliberately stale counters must be ignored.
    monkeypatch.setattr(spark, "stop", lambda: None)  # Shared pytest session only.
    profile_inputs(raw, evidence, tmp_path / "fresh")
    fresh = json.loads((tmp_path / "fresh/summary.json").read_text())
    assert fresh["rows"] == expected_rows
    assert fresh["profile_origin"] == "independent_new_snapshot_measurement"
    assert fresh["changed_tables_from_previous_snapshot"] == ["title.principals"]
    assert (
        json.loads((tmp_path / "fresh/missing_profile.json").read_text())
        == evidence["missing_profile"]
    )


def test_cloud_does_not_publish_an_incomplete_pipeline(tmp_path):
    destination = tmp_path / "published"
    with pytest.raises(FileNotFoundError):
        package(
            tmp_path / "prepared",
            tmp_path / "analysis",
            tmp_path / "credits",
            destination,
        )
    assert not destination.exists()


def test_cloud_full_pipeline_publishes_only_verified_derived_outputs(tmp_path, spark):
    """Exercise production stage boundaries without downloading real IMDb data."""
    from imdb_preparation.download import sha256
    from imdb_preparation.explore import run as explore
    from imdb_preparation.exports import export
    from imdb_preparation.prepare import prepare

    raw, measured, prepared, analysis, credits, published = (
        tmp_path / name
        for name in ("raw", "profile", "prepared", "analysis", "credits", "published")
    )
    write_fixture(raw)
    manifest_path = raw / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["scope"] = "full"  # Synthetic scope test; never evidence of full IMDb.
    manifest_path.write_text(json.dumps(manifest) + "\n")
    run(spark, raw, measured, workers=2)
    prepare(spark, raw, prepared, 2026, raw_profile=measured, input_partitions=4)
    languages = tmp_path / "languages.csv"
    languages.write_text(
        "tconst,language,source_url\ntt0000001,en,https://example.invalid/fixture\n"
    )
    config = {
        "minimum_runtime": 40,
        "minimum_votes": 1,
        "episode_minimum_votes": 1,
        "runtime_zscore": 2,
        "success_rating": 7,
        "early_career_fraction": 0.25,
        "student_validated": False,
        "as_of_year": 2026,
    }
    explore(spark, prepared, analysis, config, languages)
    export(spark, prepared, credits, selected_titles=analysis / "selected_titles.csv")
    package(prepared, analysis, credits, published)
    verified = json.loads((published / "VERIFIED.json").read_text())
    assert set(verified["questions"]) == {f"q{i}" for i in range(1, 9)} | {
        "top10_per_genre_year"
    }
    assert not verified["student_validated"]
    assert (published / "enrichment/candidates.csv").read_text().splitlines() == [
        "tconst,nconst",
        "tt0000001,nm0000001",
    ]
    assert not (published / "clean").exists() and not (published / "raw").exists()
    for relative in ("imdb_ids", "movie_catalog", "clean/title.ratings"):
        assert (published / "integration" / relative / "_SUCCESS").exists()
    for relative, expected in verified["sha256"].items():
        assert sha256(published / relative) == expected
    from imdb_preparation.finalize import run as finalize

    old_titles, old_pairs = tmp_path / "old-titles.csv", tmp_path / "old-pairs.csv"
    old_titles.write_text("tconst\ntt0000001\ntt9999999\n")
    old_pairs.write_text("tconst,nconst\ntt0000001,nm0000001\ntt9999999,nm0000001\n")
    handoff = tmp_path / "handoff"
    review = finalize(spark, prepared, published, handoff, old_titles, old_pairs)
    assert review["ready_for_student_review"] and not review["student_validated"]
    assert review["films"] == review["primary_pairs"] == review["input_pairs"] == 1
    assert review["reserve_pairs"] == review["new_people_to_lookup"] == 0
    assert review["thresholds_compared"] == 12
    assert review["changes_from_previous_inputs"] == {
        "films_added": 0,
        "films_removed": 1,
        "pairs_added": 0,
        "pairs_removed": 1,
    }
    for filename, expected in review["files_sha256"].items():
        assert sha256(handoff / filename) == expected
    measured_summary = json.loads((measured / "summary.json").read_text())
    measured_summary["manifest_sha256"] = "0" * 64
    (measured / "summary.json").write_text(json.dumps(measured_summary))
    with pytest.raises(ValueError, match="Profile preparation source mismatch"):
        package(prepared, analysis, credits, tmp_path / "wrong-profile")
    assert not (tmp_path / "wrong-profile").exists()


def test_downloaded_parent_artifact_checks_payload_without_optional_crc(tmp_path):
    from imdb_preparation.download import sha256
    from imdb_preparation.finalize import verify_published

    payload, crc = tmp_path / "data.csv", tmp_path / ".data.csv.crc"
    payload.write_text("tconst\ntt0000001\n")
    crc.write_bytes(b"Hadoop auxiliary sidecar")
    (tmp_path / "VERIFIED.json").write_text(
        json.dumps(
            {
                "scope": "full",
                "sha256": {"data.csv": sha256(payload), ".data.csv.crc": sha256(crc)},
            }
        )
    )
    crc.unlink()
    assert verify_published(tmp_path) == [".data.csv.crc"]
    payload.write_text("corrupted payload")
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_published(tmp_path)


def test_threshold_comparison_keeps_vote_duration_and_ranking_rules(spark):
    from imdb_preparation.finalize import rankings_for_thresholds

    rows = [
        (f"tt{i:07d}", 2000, ["Drama"], 90, 8.0, 10000 if i == 12 else 1000)
        for i in range(1, 13)
    ]
    rows.append(("tt0000013", 2000, ["Drama"], 60, 9.0, 10000))
    catalog = spark.createDataFrame(
        rows,
        "tconst string,startYear long,genres array<string>,runtimeMinutes long,averageRating double,numVotes long",
    )
    settings, _, _, ranked = rankings_for_thresholds(
        catalog, {"minimum_runtime": 40, "minimum_votes": 1000}
    )
    assert settings.count() == 9

    def ids(duration, votes):
        return [
            r.tconst
            for r in ranked.filter(
                f"minimum_runtime={duration} AND minimum_votes={votes}"
            )
            .orderBy("genre_year_rank")
            .collect()
        ]

    assert ids(40, 1000) == ["tt0000013", "tt0000012"] + [
        f"tt{i:07d}" for i in range(1, 9)
    ]
    assert ids(90, 1000) == ["tt0000012"] + [f"tt{i:07d}" for i in range(1, 10)]
    assert ids(40, 10000) == ["tt0000013", "tt0000012"]
    assert ids(90, 10000) == ["tt0000012"]
