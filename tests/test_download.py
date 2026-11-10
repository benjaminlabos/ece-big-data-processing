import gzip
import io
import json
import zlib

import pytest

from imdb_preparation import download as downloader


class Response(io.BytesIO):
    def __init__(self, body, content_length=None, status=200):
        super().__init__(body)
        self.status = status
        self.headers = {"ETag": '"controlled-response"'}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)


def source(monkeypatch, body, content_length=None, status=200):
    monkeypatch.setattr(
        downloader,
        "urlopen",
        lambda request, timeout: Response(body, content_length, status),
    )


def test_complete_download_rejects_short_http_body(monkeypatch, tmp_path):
    body = gzip.compress(b"header\nvalue\n")
    source(monkeypatch, body, len(body) + 20)
    destination = tmp_path / "short"
    with pytest.raises(ValueError, match="Content-Length"):
        downloader.download(destination)
    assert not (destination / "manifest.json").exists()
    assert not list(destination.glob("*.tsv.gz"))
    assert list(destination.glob("*.part"))


@pytest.mark.parametrize(
    "content_length", [True, False], ids=["with-length", "without-length"]
)
def test_complete_download_checks_gzip_footer(monkeypatch, tmp_path, content_length):
    body = gzip.compress(b"header\nvalue\n")[:-8]
    source(monkeypatch, body, len(body) if content_length else None)
    destination = tmp_path / "truncated-gzip"
    with pytest.raises((EOFError, gzip.BadGzipFile, zlib.error)):
        downloader.download(destination)
    assert not (destination / "manifest.json").exists()
    assert not list(destination.glob("*.tsv.gz"))


@pytest.mark.parametrize("body", [b"", b"not a gzip file"], ids=["empty", "not-gzip"])
def test_complete_download_rejects_non_gzip_body(monkeypatch, tmp_path, body):
    source(monkeypatch, body, len(body))
    destination = tmp_path / "not-gzip"
    with pytest.raises(ValueError, match="gzip"):
        downloader.download(destination)
    assert not (destination / "manifest.json").exists()


def test_complete_download_rejects_corrupt_crc(monkeypatch, tmp_path):
    body = bytearray(gzip.compress(b"header\nvalue\n"))
    body[-8] ^= 1
    source(monkeypatch, bytes(body), len(body))
    destination = tmp_path / "bad-crc"
    with pytest.raises(gzip.BadGzipFile, match="CRC"):
        downloader.download(destination)
    assert not (destination / "manifest.json").exists()


def test_rejects_unrequested_partial_http_response(monkeypatch, tmp_path):
    body = gzip.compress(b"header\nvalue\n")
    source(monkeypatch, body, len(body), status=206)
    destination = tmp_path / "partial-response"
    with pytest.raises(ValueError, match="HTTP 206"):
        downloader.download(destination)
    assert not (destination / "manifest.json").exists()


@pytest.mark.parametrize("with_length", [True, False])
def test_complete_manifest_records_transport_integrity(
    monkeypatch, tmp_path, with_length
):
    body = gzip.compress(b"header\nvalue\n")
    source(monkeypatch, body, len(body) if with_length else None)
    destination = tmp_path / "valid"
    downloader.download(destination)
    manifest = json.loads((destination / "manifest.json").read_text())
    assert manifest["scope"] == "full"
    assert set(manifest["files"]) == set(downloader.TABLES)
    for metadata in manifest["files"].values():
        assert metadata["bytes"] == len(body)
        assert metadata["gzip_integrity_verified"]
        assert metadata["source_compressed_content_length"] == (
            len(body) if with_length else None
        )
        assert metadata["sha256"] == downloader.sha256(destination / metadata["path"])


def test_sample_prefix_is_not_validated_as_a_complete_download(monkeypatch, tmp_path):
    body = gzip.compress(b"header\nfirst\nsecond\nthird\n")
    source(monkeypatch, body, len(body))
    destination = tmp_path / "sample"
    downloader.download(destination, rows=1)
    manifest = json.loads((destination / "manifest.json").read_text())
    assert manifest["scope"] == "sample"
    for metadata in manifest["files"].values():
        assert (destination / metadata["path"]).read_bytes() == b"header\nfirst\n"
        assert not metadata["gzip_integrity_verified"]
