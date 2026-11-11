"""Copy finalized cache records to one JSON Lines file for Spark."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import tempfile


def freeze_cache(cache, output):
    records = sorted(cache.glob("*.json"))
    if not records:
        raise ValueError(f"No cache records in {cache}")
    if output.suffix != ".json":
        raise ValueError("The snapshot filename must end in .json.")
    if output.exists() or output.resolve().is_relative_to(cache.resolve()):
        raise ValueError("Choose a new output file outside the cache directory.")
    output.parent.mkdir(parents=True, exist_ok=True)
    digest = sha256()
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".cache-snapshot-") as staging:
        staged = Path(staging) / "snapshot.json"
        with staged.open("wb") as stream:
            for record in records:
                line = record.read_bytes().rstrip(b"\r\n") + b"\n"
                stream.write(line)
                digest.update(line)
        staged.rename(output)
    return {"cache_records": len(records), "sha256": digest.hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(freeze_cache(args.cache, args.output)))
