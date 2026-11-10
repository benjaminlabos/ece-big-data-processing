import json

import pytest
from pyspark.sql import SparkSession

from imdb_preparation.download import sha256
from imdb_preparation.fixture import fixture_rows, write_fixture
from imdb_preparation.prepare import inspect_snapshot, prepare
from imdb_preparation.quality import profile
from imdb_preparation.schema import TABLES, raw_schema


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("IMDb preparation tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


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
    assert len(akas) == 1  # 01 and 1 conflict on canonical key; no arbitrary winner
    assert akas[0].title == 'Titre "cité"'
    assert akas[0].types == ["alternative", "festival"]
    assert akas[0].isOriginalTitle is None


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
