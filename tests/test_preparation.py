import json
import shutil

import pytest

from imdb_preparation.download import sha256
from imdb_preparation.fixture import fixture_rows, write_fixture
from imdb_preparation.prepare import inspect_snapshot, prepare
from imdb_preparation.quality import clean_values, normalize, partition_keys, profile
from imdb_preparation.schema import TABLES, raw_schema


@pytest.fixture(scope="module")
def prepared(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("imdb")
    raw = root / "raw"
    write_fixture(raw)
    before = {p.name: sha256(p) for p in raw.iterdir()}
    output = root / "result"
    summary = prepare(spark, raw, output, 2026)
    assert {p.name: sha256(p) for p in raw.iterdir()} == before
    return raw, output, summary


def read_table(spark, output, table):
    return spark.read.parquet(str(output / "clean" / table))


def test_all_columns_denominators_and_reconciliation(prepared):
    _, output, summary = prepared
    entries = json.loads((output / "missing_profile.json").read_text())
    assert {(x["table"], x["column"]) for x in entries} == {
        (t, c) for t, columns in TABLES.items() for c in columns
    }
    rows = fixture_rows()
    for entry in entries:
        table, column = entry["table"], entry["column"]
        index = TABLES[table].index(column)
        missing = sum(row[index].strip() in ("", "\\N") for row in rows[table])
        assert entry["total_rows"] == len(rows[table])
        assert entry["missing_rows"] == missing
        assert entry["missing_percent"] == pytest.approx(
            100 * missing / len(rows[table])
        )
    expected = sorted(
        entries, key=lambda x: (-x["missing_percent"], x["table"], x["column"])
    )[:5]
    assert json.loads((output / "top5_missing.json").read_text()) == expected
    for table, counts in summary["tables"].items():
        assert counts["raw_rows"] == len(rows[table])
        assert (
            counts["raw_rows"]
            == counts["clean_rows"]
            + counts["quarantined_rows"]
            + counts["normalized_duplicate_rows_removed"]
        )
    assert summary["tables"]["title.basics"]["normalized_duplicate_rows_removed"] == 1
    assert summary["tables"]["title.basics"]["quarantined_rows"] == 3
    assert (output / "SUCCESS.json").exists()


def test_dates_types_and_raw_string_preservation(spark, prepared):
    _, output, _ = prepared
    titles = {r.tconst: r for r in read_table(spark, output, "title.basics").collect()}
    assert titles["tt0000001"].primaryTitle == 'A title with "quotes"'
    assert titles["tt0000002"].endYear is None
    assert titles["tt0000002"].runtimeMinutes is None
    assert titles["tt0000002"].isAdult is None
    assert titles["tt0000005"].primaryTitle is None
    assert titles["tt0000005"].genres == ["Drama"]
    assert titles["tt0000006"].startYear == 2030  # announced titles are possible
    assert "tt0000004" not in titles
    people = {r.nconst: r for r in read_table(spark, output, "name.basics").collect()}
    assert people["nm0000001"].deathYear is None
    assert people["nm0000002"].birthYear is None
    rating = (
        read_table(spark, output, "title.ratings")
        .filter("tconst = 'tt0000002'")
        .first()
    )
    assert rating.averageRating is None and rating.numVotes is None
    assert (
        read_table(spark, output, "title.ratings")
        .schema["numVotes"]
        .dataType.simpleString()
        == "bigint"
    )
    akas = read_table(spark, output, "title.akas").collect()
    assert (
        len(akas) == 2
    )  # Equivalent 01/1 rows collapse instead of becoming conflicts.
    first = next(row for row in akas if row.titleId == "tt0000001")
    canonical = next(row for row in akas if row.titleId == "tt0000003")
    assert canonical.ordering == 1
    assert first.title == 'Titre "cité"'
    assert first.types == ["alternative", "festival"]
    assert first.isOriginalTitle is None


def test_references_and_audit(spark, prepared):
    _, output, _ = prepared
    crew = {r.tconst: r for r in read_table(spark, output, "title.crew").collect()}
    assert "tt9999999" not in crew
    assert crew["tt0000001"].directors == ["nm0000001"]
    assert crew["tt0000003"].directors is None
    people = (
        read_table(spark, output, "name.basics").filter("nconst = 'nm0000001'").first()
    )
    assert people.knownForTitles == ["tt0000001"]
    episodes = {
        r.tconst: r for r in read_table(spark, output, "title.episode").collect()
    }
    assert episodes["tt0000003"].seasonNumber == 0
    assert episodes["tt0000005"].parentTconst is None
    assert episodes["tt0000005"].seasonNumber is None
    principal = (
        read_table(spark, output, "title.principals")
        .filter("tconst = 'tt0000002'")
        .first()
    )
    assert principal.nconst is None
    assert (
        read_table(spark, output, "title.principals")
        .filter("tconst = 'tt0000001'")
        .first()
        .characters
        == '["Hero"]'
    )
    events = spark.read.option("recursiveFileLookup", True).parquet(
        str(output / "anomalies" / "name.basics")
    )
    event = events.filter("rule = 'death_before_birth'").first()
    assert event.original_value == "1970" and event.action == "set_null"
    quarantined = spark.read.option("recursiveFileLookup", True).parquet(
        str(output / "quarantine" / "title.basics")
    )
    assert quarantined.filter("reason = 'conflicting_primary_key'").count() == 2
    akas_events = (
        spark.read.option("recursiveFileLookup", True)
        .parquet(str(output / "anomalies" / "title.akas"))
        .collect()
    )
    assert {r.action for r in akas_events} == {"set_null", "quarantine_row"}
    assert all(json.loads(r.key_json)["ordering"] == "1" for r in akas_events)


def test_sample_does_not_treat_absent_parents_as_real_orphans(spark, prepared):
    from imdb_preparation.quality import check_reference

    _, output, _ = prepared
    child = spark.createDataFrame(
        [("tt9999999", "8.0", "10")], raw_schema("title.ratings")
    )
    parent = read_table(spark, output, "title.basics")
    retained, events, rejected = check_reference(
        "title.ratings", child, "tconst", parent, "tconst", False, False
    )
    assert retained.count() == 1 and rejected.count() == 0
    assert events.first().action == "retained_unverified"
    assert events.first().rule == "reference_not_in_snapshot"


def test_empty_and_null_profile(spark):
    empty_count, empty_profile = profile(
        "title.crew", spark.createDataFrame([], raw_schema("title.crew"))
    )
    assert empty_count == 0 and all(e["missing_percent"] is None for e in empty_profile)
    raw = spark.createDataFrame(
        [("tt0000001", None, " "), ("tt0000002", "\\N", "nm0000001")],
        raw_schema("title.crew"),
    )
    _, entries = profile("title.crew", raw)
    assert entries[1]["missing_percent"] == 100
    assert entries[2]["missing_percent"] == 50


def test_snapshot_integrity_and_safe_rerun(tmp_path, spark, prepared):
    raw, output, _ = prepared
    with pytest.raises(FileExistsError):
        prepare(spark, raw, output, 2026)
    with pytest.raises(ValueError, match="separate"):
        prepare(spark, raw, raw / "nested", 2026)
    new_raw = tmp_path / "tampered"
    write_fixture(new_raw)
    (new_raw / "title.crew.tsv").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        inspect_snapshot(new_raw)


def test_recovery_reuses_only_completed_tables_and_preserves_results(
    tmp_path, spark, prepared
):
    from imdb_preparation.prepare import json_write
    from imdb_preparation.profile_full import run as raw_profile_run

    raw, original, baseline = prepared
    checkpoint, profile_dir, recovered = (
        tmp_path / n for n in ("checkpoint", "raw-profile", "recovered")
    )
    checkpoint.mkdir()
    prefix = list(TABLES)[:5]
    for table in prefix:
        for kind in ("clean", "anomalies", "quarantine"):
            shutil.copytree(original / kind / table, checkpoint / kind / table)
    json_write(
        checkpoint / "progress.json",
        {
            "scope": "fixture",
            "completed_tables": prefix,
            "tables": {table: baseline["tables"][table] for table in prefix},
        },
    )
    raw_profile_run(spark, raw, profile_dir, workers=2)
    bad_profile = tmp_path / "wrong-snapshot-profile"
    shutil.copytree(profile_dir, bad_profile)
    altered = json.loads((bad_profile / "summary.json").read_text())
    altered["manifest_sha256"] = "0" * 64
    json_write(bad_profile / "summary.json", altered)
    json_write(bad_profile / "PROFILE_SUCCESS.json", altered)
    with pytest.raises(ValueError, match="Raw profile snapshot mismatch"):
        prepare(
            spark, raw, tmp_path / "wrong-snapshot", 2026, checkpoint, bad_profile, 2026
        )
    before = {
        str(p.relative_to(checkpoint)): sha256(p)
        for p in checkpoint.rglob("*")
        if p.is_file()
    }
    with pytest.raises(ValueError, match="original as-of year"):
        prepare(spark, raw, tmp_path / "wrong-year", 2026, checkpoint, profile_dir)
    result = prepare(spark, raw, recovered, 2026, checkpoint, profile_dir, 2026, 4)
    assert result["tables"] == baseline["tables"]
    assert result["recovery"]["reused_tables"] == prefix
    assert result["recovery"]["legacy_context_asserted"]
    assert json.loads((recovered / "missing_profile.json").read_text()) == json.loads(
        (original / "missing_profile.json").read_text()
    )
    for table in TABLES:
        expected, actual = (
            read_table(spark, original, table),
            read_table(spark, recovered, table),
        )
        assert (
            expected.exceptAll(actual).count()
            == actual.exceptAll(expected).count()
            == 0
        )
    assert before == {
        str(p.relative_to(checkpoint)): sha256(p)
        for p in checkpoint.rglob("*")
        if p.is_file()
    }
    assert (recovered / "SUCCESS.json").exists() and not (
        checkpoint / "SUCCESS.json"
    ).exists()


def test_recovery_rejects_incomplete_checkpoint(tmp_path, spark, prepared):
    from imdb_preparation.prepare import adopt_checkpoint

    _, _, baseline = prepared
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    (checkpoint / "progress.json").write_text(
        json.dumps(
            {
                "scope": "fixture",
                "completed_tables": ["title.basics"],
                "tables": {"title.basics": baseline["tables"]["title.basics"]},
            }
        )
    )
    with pytest.raises(ValueError, match="Incomplete checkpoint"):
        adopt_checkpoint(
            spark,
            checkpoint,
            tmp_path / "stage",
            {"scope": "fixture", "as_of_year": 2026},
            {"title.basics": [{"total_rows": 9}]},
            2026,
        )


def test_published_checkpoints_chain_across_jobs_without_early_success(
    tmp_path, spark, prepared
):
    from imdb_preparation.profile_full import run as raw_profile_run

    raw, original, baseline = prepared
    profile_dir = tmp_path / "profile"
    raw_profile_run(spark, raw, profile_dir, workers=2)
    previous = None
    for index, stop_after in enumerate(
        ("name.basics", "title.episode", "title.ratings")
    ):
        output = tmp_path / f"phase-{index}"
        result = prepare(
            spark,
            raw,
            output,
            2026,
            resume_from=previous,
            raw_profile=profile_dir,
            input_partitions=4,
            stop_after=stop_after,
        )
        prefix = list(TABLES)[: list(TABLES).index(stop_after) + 1]
        assert list(result["tables"]) == prefix
        assert (output / "SUCCESS.json").exists() == (index == 2)
        assert (output / "CHECKPOINT.json").exists() == (index != 2)
        assert result["checkpoint_only"] == (index != 2)
        if previous:
            assert not result["recovery"]["legacy_context_asserted"]
        if index == 0:
            from imdb_preparation.exports import load_prepared

            with pytest.raises(FileNotFoundError):
                load_prepared(spark, output)
        previous = output
    assert result["tables"] == baseline["tables"]
    for table in TABLES:
        expected = read_table(spark, original, table)
        actual = read_table(spark, previous, table)
        assert (
            expected.exceptAll(actual).count()
            == actual.exceptAll(expected).count()
            == 0
        )
    assert json.loads((previous / "missing_profile.json").read_text()) == json.loads(
        (original / "missing_profile.json").read_text()
    )


def test_malformed_tsv_fails_without_publishing_output(tmp_path, spark):
    raw, output = tmp_path / "raw", tmp_path / "output"
    write_fixture(raw)
    path = raw / "title.basics.tsv"
    with path.open("a", encoding="utf-8") as stream:
        stream.write("tt8888888\tmovie\ttoo few fields\n")
    manifest = json.loads((raw / "manifest.json").read_text())
    manifest["files"]["title.basics"]["sha256"] = sha256(path)
    (raw / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(Exception, match="MALFORMED_RECORD"):
        prepare(spark, raw, output, 2026)
    assert not output.exists()


@pytest.mark.parametrize("table", ["title.akas", "title.principals"])
def test_canonical_ordering_collapses_only_identical_rows(spark, table):
    columns = TABLES[table]
    identity = "titleId" if table == "title.akas" else "tconst"
    payload = "title" if table == "title.akas" else "category"
    base = {c: "same" for c in columns}
    base.update({identity: "tt0000001", "ordering": "01", payload: "same"})
    equivalent = {**base, "ordering": "1"}
    conflict_a = {**base, identity: "tt0000002", payload: "different A"}
    conflict_b = {**conflict_a, "ordering": "1", payload: "different B"}
    raw = spark.createDataFrame(
        [
            tuple(row[c] for c in columns)
            for row in (base, equivalent, conflict_a, conflict_b)
        ],
        raw_schema(table),
    )
    distinct, good, rejected = partition_keys(table, normalize(raw))
    assert distinct.count() == 3
    assert good.count() == 1 and good.first().ordering == "1"
    assert rejected.count() == 2
    assert {r.reason for r in rejected.collect()} == {"conflicting_primary_key"}


def test_invalid_ordering_values_remain_distinct_in_quarantine(spark):
    table = "title.akas"
    rows = [
        ("tt0000001", value, "Same", "US", "en", None, None, "0")
        for value in ("0", "x", "9223372036854775808", None)
    ]
    distinct, good, rejected = partition_keys(
        table, spark.createDataFrame(rows, raw_schema(table))
    )
    assert distinct.count() == 4 and good.count() == 0
    assert rejected.count() == 4
    assert {r.reason for r in rejected.collect()} == {"invalid_primary_key"}


def test_unicode_whitespace_is_missing_and_edges_are_normalized(spark):
    raw = spark.createDataFrame(
        [
            ("tt0000001", "\u00a0\u202f\t ", "\u2007"),
            ("tt0000002", " \u00a0\\N\u202f ", "\u00a0nm0000001\u202f"),
            ("tt0000003", "nm0000001,\u00a0nm0000002\u202f", "Name\u00a0Inside"),
        ],
        raw_schema("title.crew"),
    )
    count, entries = profile("title.crew", raw)
    assert count == 3 and entries[1]["missing_rows"] == 2
    assert entries[1]["missing_percent"] == pytest.approx(200 / 3)
    assert entries[2]["missing_rows"] == 1
    normalized = {r.tconst: r for r in normalize(raw).collect()}
    assert normalized["tt0000001"].directors is None
    assert normalized["tt0000002"].writers == "nm0000001"
    assert normalized["tt0000003"].writers == "Name\u00a0Inside"
    typed, _ = clean_values("title.crew", normalize(raw), 2026)
    assert typed.filter("tconst = 'tt0000003'").first().directors == [
        "nm0000001",
        "nm0000002",
    ]
