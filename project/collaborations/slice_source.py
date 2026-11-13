"""Copy source TSV records for selected titles while checking the gzip hashes."""

import argparse
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import uuid


TABLES = ("title.basics", "title.crew", "title.principals", "title.ratings", "name.basics")


class HashReader:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.stream.read(size)
        self.digest.update(data)
        return data


def identifier(token):
    return token.strip() if token.isascii() else token.decode("utf-8").strip().encode("utf-8")


def copy_rows(source, target, keys, table, people, expected):
    count = 0
    output_digest = hashlib.sha256()
    with source.open("rb") as raw, target.open("wb") as out:
        reader = HashReader(raw)
        with gzip.GzipFile(fileobj=reader, mode="rb") as records:
            header = records.readline()
            out.write(header)
            output_digest.update(header)
            for row in records:
                key, _, fields = row.partition(b"\t")
                if identifier(key) not in keys:
                    continue
                out.write(row)
                output_digest.update(row)
                count += 1
                if table == "title.crew":
                    people.update(identifier(value) for value in fields.split(b"\t", 1)[0].split(b","))
                if table == "title.principals":
                    columns = fields.split(b"\t", 2)
                    if len(columns) >= 2:
                        people.add(identifier(columns[1]))
        if reader.digest.hexdigest() != expected:
            raise ValueError(f"Source SHA256 mismatch for {table}")
    return {"source_sha256": expected, "sha256": output_digest.hexdigest(), "rows": count}


def make_slice(raw_dir, selected, output):
    raw_dir, output = raw_dir.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError(f"Choose a new output directory: {output}")
    if output == raw_dir or output.is_relative_to(raw_dir):
        raise ValueError("Output must be outside the input directory")
    selected_bytes = selected.read_bytes()
    lines = selected_bytes.decode("utf-8-sig").splitlines()
    records = csv.DictReader(lines)
    if records.fieldnames != ["tconst"]:
        raise ValueError("Expected exactly one column named tconst")
    title_ids = set()
    for row in records:
        value = row.get("tconst") or ""
        if None in row or not re.fullmatch(r"tt[0-9]{7,}", value):
            raise ValueError(f"Invalid selected title identifier: {value!r}")
        title_ids.add(value.encode("ascii"))

    manifest_bytes = (raw_dir / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest["scope"] != "full":
        raise ValueError("The slice must start from the complete source snapshot")
    staging = output.with_name(f"{output.name}.incomplete-{uuid.uuid4().hex[:8]}")
    staging.mkdir(parents=True)
    people = set()
    files = {}
    for table in TABLES:
        metadata = manifest["files"][table]
        source = (raw_dir / metadata["path"]).resolve()
        if source.parent != raw_dir:
            raise ValueError(f"Invalid local input path for {table}")
        keys = people if table == "name.basics" else title_ids
        files[table] = copy_rows(
            source, staging / f"{table}.tsv", keys, table, people, metadata["sha256"]
        )
        print(f"Copied {table}: {files[table]['rows']} source rows", flush=True)
    receipt = {
        "scope": "title_slice",
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "selected_titles_sha256": hashlib.sha256(selected_bytes).hexdigest(),
        "selected_titles": len(title_ids),
        "files": files,
    }
    (staging / "manifest.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
    )
    os.rename(staging, output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--selected", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    make_slice(args.raw_dir, args.selected, args.output)
