"""Snapshot Wikipedia article titles linked to film/person IMDb IDs in Wikidata."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
from io import StringIO
import json
from pathlib import Path
import re
import time
from urllib.parse import unquote, urlencode, urlsplit
from urllib.request import Request, urlopen


ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "ECE-BigData-gr04/1.0 (https://github.com/benjaminlabos/ece-big-data-processing)"
FILM_TYPES = "wd:Q11424, wd:Q24869, wd:Q93204, wd:Q24862, wd:Q29168811"


def fetch_links(language, limit):
    query = f"""
        SELECT ?item ?imdb ?article WHERE {{
          ?item wdt:P345 ?imdb .
          ?article schema:about ?item ; schema:isPartOf <https://{language}.wikipedia.org/> .
          ?item wdt:P31 ?kind .
          FILTER ((STRSTARTS(?imdb, "nm") && ?kind = wd:Q5) ||
                  (STRSTARTS(?imdb, "tt") && ?kind IN ({FILM_TYPES})))
        }} LIMIT {limit}
    """
    request = Request(
        ENDPOINT + "?" + urlencode({"query": query}),
        headers={"Accept": "text/csv", "User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=75) as response:
        rows = csv.DictReader(StringIO(response.read().decode("utf-8-sig")))
        if rows.fieldnames != ["item", "imdb", "article"]:
            raise ValueError(f"Unexpected Wikidata CSV header: {rows.fieldnames}")
        links = set()
        for row in rows:
            article = urlsplit(row["article"])
            item = row["item"].rsplit("/", 1)[-1]
            imdb = row["imdb"]
            if (article.hostname == f"{language}.wikipedia.org"
                    and article.path.startswith("/wiki/")
                    and re.fullmatch(r"Q\d+", item)
                    and re.fullmatch(r"(?:tt|nm)\d+", imdb)):
                title = unquote(article.path.removeprefix("/wiki/")).replace("_", " ")
                links.add((f"{language}wiki", title, imdb, item))
    return links


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--languages", default="en,fr,de,es")
    parser.add_argument("--limit-per-wiki", type=int, default=10000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    languages = list(dict.fromkeys(lang.strip() for lang in args.languages.split(",")))
    if not all(re.fullmatch(r"[a-z]{2,3}", lang) for lang in languages):
        parser.error("--languages must contain comma-separated Wikipedia language codes")
    if not 1 <= args.limit_per_wiki <= 50000:
        parser.error("--limit-per-wiki must be between 1 and 50000")

    links = set()
    counts = {}
    for index, language in enumerate(languages):
        if index:
            time.sleep(1)
        wiki_links = fetch_links(language, args.limit_per_wiki)
        if not wiki_links:
            raise RuntimeError(f"Wikidata returned no usable links for {language}wiki")
        counts[language] = len(wiki_links)
        links.update(wiki_links)
        print(f"{language}wiki: {len(wiki_links)} mappings", flush=True)

    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["wiki", "title", "imdb_id", "wikidata_id"])
        writer.writerows(sorted(links))
    temporary.replace(output)
    print(json.dumps({
        "source": ENDPOINT,
        "snapshot_utc": datetime.now(timezone.utc).isoformat(),
        "rows": len(links),
        "rows_per_language": counts,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "output": str(output),
    }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
