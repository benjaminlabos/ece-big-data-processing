import json

from imdb_preparation.fixture import fixture_rows, write_fixture
from imdb_preparation.profile_full import run
from imdb_preparation.schema import TABLES


def test_parallel_profile_accounts_for_all_raw_records(spark, tmp_path):
    raw = tmp_path / "raw"
    write_fixture(raw)
    output = tmp_path / "profile"
    summary = run(spark, raw, output, workers=2)
    assert (
        summary["kind"] == "raw_missingness_only" and not summary["cleaning_validated"]
    )
    assert summary["rows"] == {
        table: len(rows) for table, rows in fixture_rows().items()
    }
    entries = json.loads((output / "missing_profile.json").read_text())
    assert len(entries) == sum(len(cols) for cols in TABLES.values()) == 39
    for entry in entries:
        table = entry["table"]
        index = TABLES[table].index(entry["column"])
        expected = sum(
            row[index].strip() in ("", "\\N") for row in fixture_rows()[table]
        )
        assert entry["missing_rows"] == expected
    assert (output / "PROFILE_SUCCESS.json").exists()
    assert not (output / "SUCCESS.json").exists()
