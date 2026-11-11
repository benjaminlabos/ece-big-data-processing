import io
import json

import pytest

from imdb_preparation import languages


def test_language_collection_preserves_original_source(monkeypatch, tmp_path):
    raw = b"tconst,language,source_url\ntt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    monkeypatch.setattr(
        languages,
        "urlopen",
        lambda request, timeout: io.BytesIO(
            b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n"
        )
        if "P218" in request.full_url
        else io.BytesIO(raw),
    )
    output = tmp_path / "languages.csv"
    languages.collect(output)
    assert output.read_bytes() == raw
    metadata = json.loads(output.with_suffix(".manifest.json").read_text())
    assert metadata["rows"] == 1 and metadata["property"].endswith("Property:P364")
    assert "P364" in metadata["query"]
    with pytest.raises(FileExistsError):
        languages.collect(output)


@pytest.mark.parametrize(
    "raw", [b"<html>unavailable</html>", b"tconst,language,source_url\n"]
)
def test_language_collection_does_not_publish_invalid_response(
    monkeypatch, tmp_path, raw
):
    monkeypatch.setattr(languages, "urlopen", lambda request, timeout: io.BytesIO(raw))
    output = tmp_path / "languages.csv"
    with pytest.raises(ValueError):
        languages.collect(output)
    assert not output.exists()


def test_iso_aliases_do_not_create_extra_languages(spark):
    from imdb_preparation.explore import apply_language_vocabulary

    statements = spark.createDataFrame(
        [
            ("tt0000001", "Q1", "https://example.invalid/film"),
            ("tt0000001", "Q2", "https://example.invalid/film"),
            ("tt0000001", "Q3", "https://example.invalid/film"),
        ],
        "tconst string,language string,source_url string",
    )
    vocabulary = spark.createDataFrame(
        [("Q1", "en"), ("Q1", "eng"), ("Q2", "zxx")], "language string,iso_code string"
    )
    mapped, unknown = apply_language_vocabulary(statements, vocabulary)
    assert [r.language for r in mapped.collect()] == ["en"]
    assert unknown.count() == 2
