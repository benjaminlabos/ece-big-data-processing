"""Create splittable copies of verified IMDb inputs on the local Spark disk."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from slice_source import TABLES


def digest(path):
    checksum = sha256()
    with path.open("rb") as stream:
        while block := stream.read(4 << 20):
            checksum.update(block)
    return checksum.hexdigest()


def expand(raw, output):
    if output.exists() or output.resolve().is_relative_to(raw.resolve()):
        raise ValueError("Choose a fresh output directory outside the source.")
    manifest_bytes = (raw / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    if manifest.get("scope") != "full":
        raise ValueError("Expected the complete IMDb snapshot.")
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=output.parent, prefix=".expanded-") as work:
        stage = Path(work) / "inputs"
        stage.mkdir()

        def expand_one(table):
            source = raw / f"{table}.tsv.gz"
            if digest(source) != manifest["files"][table]["sha256"]:
                raise ValueError(f"Compressed source checksum mismatch: {table}")
            unpacked = sha256()
            with gzip.open(source, "rb") as stream, (stage / f"{table}.tsv").open("wb") as target:
                while block := stream.read(4 << 20):
                    target.write(block)
                    unpacked.update(block)
            print(f"Expanded {table}", flush=True)
            return table, unpacked.hexdigest()

        with ThreadPoolExecutor(max_workers=2) as pool:
            files = dict(pool.map(expand_one, TABLES))
        (stage / "manifest.json").write_text(json.dumps({
            "scope": "expanded_full_sources",
            "source_manifest_sha256": sha256(manifest_bytes).hexdigest(),
            "files": files,
        }, indent=2) + "\n", encoding="utf-8")
        stage.rename(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    expand(args.raw_dir, args.output)
