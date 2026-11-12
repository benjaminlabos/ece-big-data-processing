"""Project the requested (film, person) credits from the verified preparation."""

from collections import defaultdict
import csv
from hashlib import sha256
import json
from pathlib import Path
import sys

import pyarrow.dataset as ds


def main():
    prepared, request, output = map(Path, sys.argv[1:])
    source_hashes = {
        "summary.json": "c6ba4f0b9b77110079f65b91cea6b8da4066ba4c1f11eee8836ea823fc03ae74",
        "input_manifest.json": "aeb5effe20cab570e20fbd3a250309a0f081f7f3a203d02dfafca5da0c999cfd",
    }
    assert (prepared / "SUCCESS.json").is_file()
    for name, expected in source_hashes.items():
        assert sha256((prepared / name).read_bytes()).hexdigest() == expected, name
    requested_hash = sha256(request.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    assert requested_hash == "067eb5bf0871b4a115b12cfe799e465815601bf95252c84e2bb111c9f69fc75f"
    with request.open(newline="", encoding="utf-8") as stream:
        pairs = [(row["tconst"], row["nconst"]) for row in csv.DictReader(stream)]
    requested = set(pairs)
    assert len(pairs) == len(requested) == 1060
    dataset = ds.dataset(prepared / "clean/title.principals", format="parquet", exclude_invalid_files=True)
    projection = defaultdict(set)
    for batch in dataset.to_batches(
        columns=["tconst", "nconst", "category", "ordering"],
        filter=ds.field("tconst").isin(sorted({title for title, _ in requested})),
    ):
        for row in batch.to_pylist():
            pair = row["tconst"], row["nconst"]
            if pair in requested:
                projection[pair].add((int(row["ordering"]), row["category"]))
    assert set(projection) == requested, f"Missing credits: {sorted(requested - projection.keys())}"
    output.mkdir()
    csv_path = output / "credits.csv"
    with csv_path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["tconst", "nconst", "category", "ordering"])
        for title, person in sorted(requested):
            for ordering, category in sorted(projection[title, person]):
                writer.writerow([title, person, category, ordering])
    proof = {
        "prepared_source_run": 43093003953,
        "prepared_source_commit": "321c07d578fc77daafdf3d2a4553899af40c4ea5",
        "prepared_source_sha256": source_hashes,
        "requested_pairs": len(requested),
        "requested_pairs_sha256_lf": requested_hash,
        "matched_pairs": len(projection),
        "credit_rows": sum(map(len, projection.values())),
        "credits_sha256": sha256(csv_path.read_bytes()).hexdigest(),
    }
    (output / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
