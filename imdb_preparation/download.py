"""Fetch immutable local snapshots; limited downloads are explicitly samples.

This module only transfers source bytes. All transformations run in Spark.
"""

import argparse
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from .schema import TABLES

BASE_URL = "https://datasets.imdbws.com/"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_gzip(path):
    """Read to EOF to check every gzip member's checksum and footer."""
    with Path(path).open("rb") as source:
        if source.read(2) != b"\x1f\x8b":
            raise ValueError(f"Downloaded file is not gzip: {path}")
    with gzip.open(path, "rb") as source:
        for _ in iter(lambda: source.read(1024 * 1024), b""):
            pass


def download(destination, rows=None):
    destination = Path(destination)
    # A new snapshot directory avoids mixing files from different daily releases.
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {
        "scope": "sample" if rows is not None else "full",
        "rows_per_table_limit": rows,
        "sampling": "first N source records per table; not representative; not referentially closed"
        if rows is not None
        else None,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
    }
    try:
        for table in TABLES:
            url = BASE_URL + table + ".tsv.gz"
            suffix = ".tsv" if rows is not None else ".tsv.gz"
            target = destination / (table + suffix)
            partial = target.with_name(target.name + ".part")
            request = Request(
                url, headers={"User-Agent": "ECE-gr04-IMDb-preparation/1.0"}
            )
            with urlopen(request, timeout=90) as response, partial.open("wb") as out:
                if response.status != 200:
                    raise ValueError(f"Unexpected HTTP {response.status} for {url}")
                advertised_length = response.headers.get("Content-Length")
                expected_bytes = (
                    int(advertised_length) if advertised_length is not None else None
                )
                headers = {
                    k: response.headers.get(k) for k in ["ETag", "Last-Modified"]
                }
                if rows is not None:
                    with gzip.GzipFile(fileobj=response) as source:
                        for _ in range(rows + 1):  # include the header
                            line = source.readline()
                            if not line:
                                break
                            out.write(line)
                else:
                    received_bytes = 0
                    for chunk in iter(lambda: response.read(1024 * 1024), b""):
                        out.write(chunk)
                        received_bytes += len(chunk)
                    if expected_bytes is not None and received_bytes != expected_bytes:
                        raise ValueError(
                            f"Content-Length mismatch for {url}: expected {expected_bytes} bytes, received {received_bytes}"
                        )
            if rows is None:
                # Some servers omit Content-Length; gzip EOF/CRC checks are still required.
                verify_gzip(partial)
            os.replace(partial, target)
            manifest["files"][table] = {
                "path": target.name,
                "url": url,
                "sha256": sha256(target),
                "bytes": target.stat().st_size,
                "source_compressed_content_length": expected_bytes,
                "gzip_integrity_verified": rows is None,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                **headers,
            }
            print(f"Downloaded {target}", flush=True)
        manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
        (destination / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
    except BaseException:
        # No manifest means incomplete snapshot; prepare refuses it.
        print(
            f"Incomplete snapshot: remove {destination} or use a new directory before retrying.",
            flush=True,
        )
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument(
        "--rows", type=int, help="First N data rows per table; omit for full gz files"
    )
    args = parser.parse_args()
    if args.rows is not None and args.rows < 1:
        parser.error("--rows must be positive")
    download(args.destination, args.rows)


if __name__ == "__main__":
    main()
