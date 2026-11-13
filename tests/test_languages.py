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
    "raw",
    [
        b"<html>unavailable</html>",
        b"tconst,language,source_url\n",
        b"tconst,language,source_url\nt",
        b'tconst,language,source_url\n"tt0000001',
    ],
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


def test_language_pages_reconcile_and_preserve_source_rows(monkeypatch, tmp_path):
    from urllib.parse import parse_qs, urlparse

    monkeypatch.setattr(languages.time, "sleep", lambda delay: None)
    header = b"tconst,language,source_url\n"
    entries = [
        b"tt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n",
        b"tt0000002,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q200\n",
    ]

    def response(request, timeout):
        query = parse_qs(urlparse(request.full_url).query)["query"][0]
        if "P218" in query:
            return io.BytesIO(
                b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n"
            )
        offset = int(query.rsplit("OFFSET ", 1)[1])
        return io.BytesIO(header + (entries[offset] if offset < len(entries) else b""))

    monkeypatch.setattr(languages, "urlopen", response)
    output = tmp_path / "paged.csv"
    metadata = languages.collect(output, page_size=1)
    assert output.read_bytes() == header + b"".join(entries)
    assert metadata["rows"] == 2
    assert [page["rows"] for page in metadata["pages"]] == [1, 1, 0]
    assert languages.validate_snapshot(output) == metadata


def test_truncated_response_is_retried_before_publication(monkeypatch, tmp_path):
    monkeypatch.setattr(languages.time, "sleep", lambda delay: None)
    header = b"tconst,language,source_url\n"
    complete = (
        header
        + b"tt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    )
    responses = iter(
        [
            header + b"t",
            complete,
            b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n",
        ]
    )
    monkeypatch.setattr(
        languages, "urlopen", lambda request, timeout: io.BytesIO(next(responses))
    )
    output = tmp_path / "recovered.csv"
    metadata = languages.collect(output)
    assert output.read_bytes() == complete and metadata["rows"] == 1
    assert (
        languages.validate_snapshot(output)["collection_method"] == "ordered_pagination"
    )


def test_repeated_language_page_is_not_published(monkeypatch, tmp_path):
    monkeypatch.setattr(languages.time, "sleep", lambda delay: None)
    raw = b"tconst,language,source_url\ntt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    monkeypatch.setattr(languages, "urlopen", lambda request, timeout: io.BytesIO(raw))
    output = tmp_path / "overlap.csv"
    with pytest.raises(ValueError, match="overlap"):
        languages.collect(output, page_size=1)
    assert not output.exists()


def test_csv_order_is_not_confused_with_rdf_term_order(monkeypatch, tmp_path):
    monkeypatch.setattr(languages.time, "sleep", lambda delay: None)
    raw = (
        b"tconst,language,source_url\n"
        b"tt0000001,node123,http://www.wikidata.org/entity/Q100\n"
        b"tt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    )
    monkeypatch.setattr(
        languages,
        "urlopen",
        lambda request, timeout: io.BytesIO(
            b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n"
            if "P218" in request.full_url
            else raw
        ),
    )
    output = tmp_path / "rdf.csv"
    assert languages.collect(output)["rows"] == 2
    assert output.read_bytes() == raw


def test_historical_reference_archive_preserves_original_provenance(tmp_path):
    import hashlib
    import tarfile

    csv_path = tmp_path / "original-languages-2026-11-11.csv"
    csv_path.write_bytes(
        b"tconst,language,source_url\ntt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    )
    vocab = tmp_path / "original-languages-2026-11-11-vocabulary.csv"
    vocab.write_bytes(b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n")
    metadata = {
        "rows": 1,
        "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "property": "https://www.wikidata.org/wiki/Property:P364",
        "vocabulary_path": vocab.name,
        "vocabulary_sha256": hashlib.sha256(vocab.read_bytes()).hexdigest(),
    }
    manifest = csv_path.with_suffix(".manifest.json")
    manifest.write_text(json.dumps(metadata))
    archive = tmp_path / "reference.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        for path in (csv_path, vocab, manifest):
            stream.add(path, arcname=path.name)
    restored, actual = languages.restore_reference_archive(
        archive, tmp_path / "restored"
    )
    assert restored.read_bytes() == csv_path.read_bytes() and actual == metadata
    # The old snapshot is not falsely labelled as collected by the new pager.
    assert "collection_method" not in actual


def test_reference_archive_rejects_unexpected_paths(tmp_path):
    import tarfile

    archive = tmp_path / "unsafe.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        member = tarfile.TarInfo("../escape")
        member.size = 1
        stream.addfile(member, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="archive members"):
        languages.restore_reference_archive(archive, tmp_path / "output")
    assert not (tmp_path / "escape").exists()


def test_temporary_connection_close_is_retried(monkeypatch, tmp_path):
    from http.client import RemoteDisconnected

    monkeypatch.setattr(languages.time, "sleep", lambda delay: None)
    raw = b"tconst,language,source_url\ntt0000001,http://www.wikidata.org/entity/Q150,http://www.wikidata.org/entity/Q100\n"
    responses = iter(
        [
            RemoteDisconnected("closed"),
            raw,
            b"language,iso_code\nhttp://www.wikidata.org/entity/Q150,fr\n",
        ]
    )

    def response(request, timeout):
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return io.BytesIO(value)

    monkeypatch.setattr(languages, "urlopen", response)
    output = tmp_path / "connection.csv"
    assert languages.collect(output)["rows"] == 1
    assert output.read_bytes() == raw
