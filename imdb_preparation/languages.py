"""Collect Wikidata P364 in bounded, validated CSV pages with provenance."""

import argparse
import csv
import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ENDPOINT = "https://query.wikidata.org/sparql"
QUERY = """SELECT DISTINCT ?tconst ?language ?source_url WHERE {
  ?source_url wdt:P345 ?tconst ; wdt:P364 ?language .
  FILTER(STRSTARTS(?tconst, "tt"))
}"""
VOCABULARY_QUERY = """SELECT ?language ?iso_code WHERE {
 ?language (wdt:P218|wdt:P219|wdt:P220) ?iso_code .
}"""
COLUMNS = ["tconst", "language", "source_url"]


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_records(body, columns, allow_empty=False):
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""), strict=True)
    try:
        if next(reader, None) != columns:
            raise ValueError("Unexpected Wikidata CSV response header")
        rows = []
        for row in reader:
            if len(row) != len(columns):
                raise ValueError(
                    f"Incomplete Wikidata CSV record at line {reader.line_num}"
                )
            rows.append(row)
    except (csv.Error, UnicodeError) as error:
        raise ValueError("Malformed Wikidata CSV response") from error
    if not rows and not allow_empty:
        raise ValueError("Empty Wikidata CSV response")
    return rows


def request_csv(query, columns, allow_empty=False):
    request = Request(
        ENDPOINT + "?" + urlencode({"query": query}),
        headers={
            "Accept": "text/csv",
            "User-Agent": "ECE-gr04-IMDb-OriginalLanguages/1.0 (https://github.com/benjaminlabos/ece-big-data-processing)",
        },
    )
    for attempt in range(3):
        delay = 2**attempt
        try:
            with urlopen(request, timeout=90) as response:
                body = response.read()
                expected = getattr(response, "headers", {}).get("Content-Length")
                if expected is not None and len(body) != int(expected):
                    raise ValueError("Incomplete Wikidata HTTP response")
            return body, csv_records(body, columns, allow_empty)
        except (ValueError, OSError) as error:
            if isinstance(error, HTTPError):
                retry_after = error.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, int(retry_after))
                    except ValueError:
                        delay = max(
                            delay,
                            (
                                parsedate_to_datetime(retry_after)
                                - datetime.now(timezone.utc)
                            ).total_seconds(),
                        )
            if attempt == 2:
                raise
            time.sleep(max(1, delay))
    raise AssertionError("Unreachable")


def collect_vocabulary(output):
    path = Path(output).with_name(Path(output).stem + "-vocabulary.csv")
    body, _ = request_csv(VOCABULARY_QUERY, ["language", "iso_code"])
    temporary = path.with_name(path.name + ".part")
    temporary.write_bytes(body)
    os.replace(temporary, path)
    return {
        "vocabulary_path": path.name,
        "vocabulary_sha256": sha256(path),
        "vocabulary_query": VOCABULARY_QUERY,
    }


def validate_snapshot(path, require_paginated=True):
    path = Path(path)
    records = csv_records(path.read_bytes(), COLUMNS)
    metadata = json.loads(path.with_suffix(".manifest.json").read_text())
    if metadata["sha256"] != sha256(path) or metadata["rows"] != len(records):
        raise ValueError("Language snapshot provenance/count mismatch")
    if require_paginated and metadata.get("collection_method") != "ordered_pagination":
        raise ValueError("A paginated language snapshot is required")
    vocabulary = path.with_name(metadata["vocabulary_path"])
    csv_records(vocabulary.read_bytes(), ["language", "iso_code"])
    if sha256(vocabulary) != metadata["vocabulary_sha256"]:
        raise ValueError("Language vocabulary provenance mismatch")
    return metadata


def collect(output, page_size=20000):
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    if page_size < 1:
        raise ValueError("Positive page size required")
    output.parent.mkdir(parents=True, exist_ok=True)
    pages, bodies, total = [], [], 0
    seen_pages = set()
    for page in range(100):
        query = (
            QUERY
            + f"\nORDER BY ?tconst ?language ?source_url\nLIMIT {page_size} OFFSET {page * page_size}"
        )
        body, records = request_csv(query, COLUMNS, allow_empty=True)
        if len(records) > page_size:
            raise ValueError("Wikidata ignored the page limit")
        page_sha256 = hashlib.sha256(body).hexdigest()
        # SPARQL orders RDF terms, not their serialized CSV strings (e.g. blank
        # nodes precede IRIs). Spark handles duplicate/invalid source statements.
        if records and page_sha256 in seen_pages:
            raise ValueError("Repeated language page (pagination overlap)")
        seen_pages.add(page_sha256)
        pages.append(
            {
                "offset": page * page_size,
                "rows": len(records),
                "response_sha256": page_sha256,
            }
        )
        bodies.append(body)
        total += len(records)
        print(
            f"Original languages page {page + 1}: {len(records)} rows ({total} collected)",
            flush=True,
        )
        if len(records) < page_size:
            break
        time.sleep(1)
    else:
        raise ValueError("Language pagination exceeded its bounded page budget")
    if total == 0:
        raise ValueError("No original-language statements returned")
    assembled = bodies[0]
    for body in bodies[1:]:
        if not assembled.endswith(b"\n"):
            assembled += b"\n"
        assembled += body.partition(b"\n")[2]
    if len(csv_records(assembled, COLUMNS)) != total:
        raise ValueError("Collected language pages do not reconcile")
    temporary = output.with_name(output.name + ".part")
    temporary.write_bytes(assembled)
    time.sleep(1)
    vocabulary_metadata = collect_vocabulary(output)
    metadata = {
        "source": ENDPOINT,
        "property": "https://www.wikidata.org/wiki/Property:P364",
        "query": QUERY,
        "collection_method": "ordered_pagination",
        "page_size": page_size,
        "pages": pages,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "rows": total,
        "sha256": sha256(temporary),
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "coverage": "Observed Wikidata statements, not an exhaustive IMDb-language inventory; paginated requests are not an atomic database snapshot",
        **vocabulary_metadata,
    }
    output.with_suffix(".manifest.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, output)
    print(json.dumps(metadata, indent=2))
    return metadata


def restore_reference_archive(archive, destination):
    """Restore the known snapshot, preserving its original bytes and provenance."""
    import tarfile

    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    names = {
        "original-languages-2026-11-11.csv",
        "original-languages-2026-11-11.manifest.json",
        "original-languages-2026-11-11-vocabulary.csv",
    }
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        if {m.name for m in members} != names or len(members) != len(names):
            raise ValueError("Unexpected reference archive members")
        for member in members:
            if not member.isfile() or member.name not in names:
                raise ValueError("Unsafe reference archive member")
            with source.extractfile(member) as stream:
                (destination / member.name).write_bytes(stream.read())
    path = destination / "original-languages-2026-11-11.csv"
    metadata = validate_snapshot(path, require_paginated=False)
    if metadata.get("property") != "https://www.wikidata.org/wiki/Property:P364":
        raise ValueError("Archived source is not the original-language property")
    return path, metadata


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument(
        "--restore-archive",
        type=Path,
        help="Restore a verified historical reference into the --output directory",
    )
    a = p.parse_args()
    if a.restore_archive:
        path, metadata = restore_reference_archive(a.restore_archive, a.output)
        print(json.dumps({"path": str(path), "metadata": metadata}, indent=2))
    else:
        collect(a.output)
