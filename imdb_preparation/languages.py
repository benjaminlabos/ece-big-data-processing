"""Gather original-language statements (Wikidata P364), not alternative-title languages.

Collection only. Spark enforces the schema and joins this snapshot in explore.py.
"""

import argparse
import csv
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .download import sha256

ENDPOINT = "https://query.wikidata.org/sparql"
QUERY = """SELECT ?tconst ?language ?source_url WHERE {
  ?source_url wdt:P345 ?tconst ; wdt:P364 ?language .
  FILTER(STRSTARTS(?tconst, "tt"))
}"""
VOCABULARY_QUERY = """SELECT ?language ?iso_code WHERE {
 ?language (wdt:P218|wdt:P219|wdt:P220) ?iso_code .
}"""


def collect_vocabulary(output):
    path = Path(output).with_name(Path(output).stem + "-vocabulary.csv")
    request = Request(
        ENDPOINT + "?" + urlencode({"query": VOCABULARY_QUERY}),
        headers={
            "Accept": "text/csv",
            "User-Agent": "ECE-gr04-IMDb-OriginalLanguages/1.0 (https://github.com/benjaminlabos/ece-big-data-processing)",
        },
    )
    with urlopen(request, timeout=90) as response:
        body = response.read()
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig")))
    if next(reader, None) != ["language", "iso_code"]:
        raise ValueError("Unexpected language vocabulary response")
    if sum(1 for _ in reader) == 0:
        raise ValueError("Empty language vocabulary")
    temporary = path.with_name(path.name + ".part")
    temporary.write_bytes(body)
    os.replace(temporary, path)
    return {
        "vocabulary_path": path.name,
        "vocabulary_sha256": sha256(path),
        "vocabulary_query": VOCABULARY_QUERY,
    }


def collect(output):
    output = Path(output)
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    url = ENDPOINT + "?" + urlencode({"query": QUERY})
    request = Request(
        url,
        headers={
            "Accept": "text/csv",
            "User-Agent": "ECE-gr04-IMDb-OriginalLanguages/1.0 (https://github.com/benjaminlabos/ece-big-data-processing)",
        },
    )
    with urlopen(request, timeout=90) as response:
        body = response.read()
    reader = csv.reader(io.StringIO(body.decode("utf-8-sig")))
    if next(reader, None) != ["tconst", "language", "source_url"]:
        raise ValueError("Unexpected Wikidata response header")
    rows = sum(1 for _ in reader)
    if rows == 0:
        raise ValueError("No original-language statements returned")
    temporary = output.with_name(output.name + ".part")
    temporary.write_bytes(body)
    time.sleep(1)
    vocabulary_metadata = collect_vocabulary(output)
    metadata = {
        "source": ENDPOINT,
        "property": "https://www.wikidata.org/wiki/Property:P364",
        "query": QUERY,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "sha256": sha256(temporary),
        "license": "https://creativecommons.org/publicdomain/zero/1.0/",
        "coverage": "Wikidata statements only; not exhaustive for IMDb; language values are entity URIs",
        **vocabulary_metadata,
    }
    output.with_suffix(".manifest.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, output)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", required=True, type=Path)
    a = p.parse_args()
    collect(a.output)
