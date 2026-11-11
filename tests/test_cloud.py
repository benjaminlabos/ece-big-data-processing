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
