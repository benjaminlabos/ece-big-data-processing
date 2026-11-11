import csv
import json

import pytest

from imdb_preparation.download import sha256
from imdb_preparation.exports import (
    REFERENCE_COLUMNS,
    export,
    filter_reference,
    load_prepared,
    single_csv,
    unique_parent,
)


def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)


@pytest.fixture(scope="module")
def export_inputs(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("export-inputs")
    prepared = root / "prepared"
    tables = {
        "title.basics": spark.createDataFrame(
            [
                ("tt0000001", "movie", 'A, "quoted" film', 2000, 120, ["Drama"]),
                ("tt0000002", "movie", "Unrated", 2025, 100, None),
                ("tt0000003", "movie", "Older", 1979, 90, ["Drama"]),
                ("tt0000004", "tvMovie", "Television", 2000, 90, ["Drama"]),
                ("tt0000005", "movie", "Future", 2026, 90, ["Drama"]),
                ("tt0000006", "movie", "Unknown year", None, 90, ["Drama"]),
            ],
            "tconst string, titleType string, primaryTitle string, startYear long, runtimeMinutes long, genres array<string>",
        ),
        "name.basics": spark.createDataFrame(
            [(f"nm000000{i}",) for i in range(1, 5)], "nconst string"
        ),
        "title.principals": spark.createDataFrame(
            [
                ("tt0000001", 1, "nm0000001", "actor"),
                ("tt0000001", 2, "nm0000001", "director"),
                ("tt0000001", 3, "nm0000002", "actor"),
                ("tt0000001", 4, "nm0000003", "actress"),
                ("tt0000001", 5, "nm9999999", "actor"),
                ("tt0000001", 6, None, "writer"),
                ("tt0000002", 1, "nm0000004", "actor"),
                ("tt0000002", 2, "nm0000004", "writer"),
                ("tt0000003", 1, "nm0000001", "actor"),
            ],
            "tconst string, ordering long, nconst string, category string",
        ),
        "title.ratings": spark.createDataFrame(
            [
                ("tt0000001", 8.5, 100),
                ("tt0000003", 7.0, 10),
            ],
            "tconst string, averageRating double, numVotes long",
        ),
    }
    for table, frame in tables.items():
        frame.write.parquet(str(prepared / "clean" / table))
    # Deliberately exercise the report-only sample contract with controlled data.
    (prepared / "summary.json").write_text(
        json.dumps({"scope": "sample", "tables": {t: {} for t in tables}})
    )
    (prepared / "SUCCESS.json").write_text(json.dumps({"scope": "sample"}))
    selected = root / "selected.csv"
    write_csv(
        selected,
        ["tconst"],
        [
            ("tt0000001",),
            (" tt0000001 ",),
            ("tt0000002",),
            ("tt0000003",),
            ("tt0000004",),
            ("tt0000005",),
            ("tt0000006",),
            ("tt9999999",),
            ("bad",),
            ("",),
        ],
    )
    reference = root / "reference.csv"
    write_csv(
        reference,
        REFERENCE_COLUMNS,
        [
            ("enwiki", 'A, "quoted" title', "tt0000001", "Q100"),
            ("frwiki", "Titre cité", "tt0000001", "Q100"),
            ("enwiki", "Actor", "nm0000001", "Q101"),
            ("enwiki", "Actor", "nm0000001", "Q101"),
            ("enwiki", "Absent", "tt9999999", "Q102"),
            ("en", "Invalid wiki", "nm0000002", "Q103"),
            ("enwiki", "Invalid Q", "nm0000002", "invalid"),
            ("enwiki", "Missing Q", "nm0000003", ""),
            ("enwiki", "Future title", "tt0000005", "Q104"),
        ],
    )
    return root, prepared, selected, reference


@pytest.fixture(scope="module")
def exported(spark, export_inputs):
    root, prepared, selected, reference = export_inputs
    before = {str(p): sha256(p) for p in prepared.rglob("*") if p.is_file()}
    output = root / "output"
    summary = export(
        spark,
        prepared,
        output,
        selected,
        reference,
        "https://example.test/controlled-reference",
    )
    assert before == {str(p): sha256(p) for p in prepared.rglob("*") if p.is_file()}
    return output, summary


def test_movie_catalog_retains_unrated_and_avoids_join_multiplication(spark, exported):
    output, summary = exported
    assert (
        summary["scope"] == "sample"
        and summary["selection_method"] == "external_tconst_list"
    )
    assert summary["imdb_identifiers"] == 10
    assert summary["movies_1980_2025"] == 2
    rows = {
        r.tconst: r for r in spark.read.parquet(str(output / "movie_catalog")).collect()
    }
    assert set(rows) == {"tt0000001", "tt0000002"}
    assert rows["tt0000002"].averageRating is None
    assert rows["tt0000001"].genres == ["Drama"]
    assert (output / "SUCCESS.json").exists()


def test_enrichment_csv_roles_missing_people_and_coverage(spark, exported):
    output, summary = exported
    with (output / "candidates.csv").open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == ["tconst", "nconst"]
        pairs = {(r["tconst"], r["nconst"]) for r in reader}
    assert pairs == {
        ("tt0000001", "nm0000001"),
        ("tt0000001", "nm0000002"),
        ("tt0000001", "nm0000003"),
        ("tt0000002", "nm0000004"),
    }
    metrics = summary["enrichment"]
    assert metrics["requested_rows"] == 10 and metrics["distinct_requested_titles"] == 9
    assert (
        metrics["candidate_pairs"] == 4
        and metrics["duplicate_credit_pairs_collapsed"] == 2
    )
    assert metrics["rejected_credit_rows"] == 2
    assert metrics["principal_rows"] == 8
    assert metrics["duplicate_selection_rows_collapsed"] == 1
    assert metrics["consumer_ready"]
    assert metrics["status_counts"] == {
        "selected": 2,
        "missing_or_outside_year_range": 3,
        "not_movie": 1,
        "not_in_prepared_titles": 1,
        "invalid_identifier": 2,
    }
    coverage = {
        r.tconst: r
        for r in spark.read.parquet(str(output / "enrichment_coverage")).collect()
    }
    assert coverage["tt0000001"].candidate_people == 3
    assert coverage["tt0000002"].fewer_than_three_candidates
    assert metrics["films_with_fewer_than_three_candidates"] == 1
    rejected = spark.read.parquet(str(output / "rejected_credits"))
    assert {r.reason for r in rejected.select("reason").distinct().collect()} == {
        "missing_person_identifier",
        "person_not_in_prepared_names",
    }


def test_reference_csv_standard_quotes_unicode_and_sample_reconciliation(exported):
    output, summary = exported
    with (output / "streaming_reference.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == REFERENCE_COLUMNS
        rows = list(reader)
    assert len(rows) == 5
    assert any(r["title"] == 'A, "quoted" title' for r in rows)
    assert any(r["title"] == "Titre cité" for r in rows)
    assert any(
        r["imdb_id"] == "tt0000005" for r in rows
    )  # streaming uses all clean IDs, not only selected films
    metrics = summary["streaming_reference"]
    assert metrics["input_rows"] == 9
    assert metrics["matched_rows"] == 5 and metrics["duplicate_rows_collapsed"] == 1
    assert metrics["rejected_rows"] == 3
    assert metrics["rejection_counts"] == {
        "invalid_reference": 2,
        "identifier_not_in_prepared_snapshot": 1,
    }
    assert metrics["absent_identifiers_are_unverified"] and metrics["consumer_ready"]


def test_invalid_duplicate_parents_fail_before_multiplying_join(spark):
    duplicate = spark.createDataFrame([("tt0000001",), ("tt0000001",)], "tconst string")
    with pytest.raises(ValueError, match="Duplicate parent key"):
        unique_parent(duplicate, "tconst", "tt", "controlled")
    invalid = spark.createDataFrame([(None,)], "tconst string")
    with pytest.raises(ValueError, match="Invalid parent key"):
        unique_parent(invalid, "tconst", "tt", "controlled")


def test_empty_reference_is_exported_as_header_only(spark, tmp_path):
    reference = spark.createDataFrame(
        [("enwiki", "Unknown", "tt9999999", "Q100")],
        ", ".join(c + " string" for c in REFERENCE_COLUMNS),
    )
    ids = spark.createDataFrame([("tt0000001",)], "imdb_id string")
    matched, rejected, _ = filter_reference(reference, ids)
    assert rejected.count() == 1 and matched.count() == 0
    path = tmp_path / "empty.csv"
    single_csv(matched, path)
    assert path.read_text().strip() == ",".join(REFERENCE_COLUMNS)


def test_reference_titles_use_the_streaming_title_key(spark):
    reference = spark.createDataFrame(
        [
            ("enwiki", title, "tt0000001", "Q100")
            for title in ("___", " _ __ ", "__A__title__", "A title")
        ],
        ", ".join(c + " string" for c in REFERENCE_COLUMNS),
    )
    ids = spark.createDataFrame([("tt0000001",)], "imdb_id string")
    matched, rejected, before_dedup = filter_reference(reference, ids)
    assert [r.title for r in matched.collect()] == ["A title"]
    assert before_dedup.count() == 2
    assert rejected.count() == 2
    assert {r.reason for r in rejected.collect()} == {"invalid_reference"}
    assert {r.title for r in rejected.collect()} == {""}


def test_reruns_and_incomplete_input_are_safe(spark, tmp_path, export_inputs, exported):
    _, prepared, _, _ = export_inputs
    output, _ = exported
    with pytest.raises(FileExistsError):
        export(spark, prepared, output)
    with pytest.raises(ValueError, match="separate"):
        export(spark, prepared, prepared / "nested")
    with pytest.raises(FileNotFoundError):
        load_prepared(spark, tmp_path)
    catalog_only = tmp_path / "catalog-only"
    summary = export(spark, prepared, catalog_only)
    assert summary["selection_method"] == "not_requested"
    assert summary["movies_1980_2025"] == 2
    assert "enrichment" not in summary and "streaming_reference" not in summary
    assert not (catalog_only / "candidates.csv").exists()
    assert not (catalog_only / "streaming_reference.csv").exists()
