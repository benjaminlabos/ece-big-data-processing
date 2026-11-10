"""Collect revision-pinned Wikipedia HTML for IMDb IDs via Wikidata P345."""

import argparse
import csv
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import islice
import json
import os
from pathlib import Path
import time

import requests

WIKIDATA_API = "https://query.wikidata.org/sparql"
WIKIPEDIA = "https://en.wikipedia.org"
RETRYABLE_STATUS = {429, 502, 503, 504}
RETRYABLE_API_ERROR = {"maxlag", "ratelimited", "readonly"}


def retry_seconds(value, attempt):
    """Honor Retry-After whether it gives seconds or a date."""
    if not value:
        return 2 ** attempt
    if value.isdecimal():
        return int(value)
    try:
        deadline = parsedate_to_datetime(value)
        return max(0, (deadline - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return 2 ** attempt


def get_json(session, url, params):
    for attempt in range(3):
        time.sleep(1)
        try:
            response = session.get(url, params=params, timeout=(10, 45))
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
            continue
        if response.status_code not in RETRYABLE_STATUS:
            response.raise_for_status()
            result = response.json()
            if result.get("error", {}).get("code") not in RETRYABLE_API_ERROR:
                return result
        wait = retry_seconds(response.headers.get("Retry-After"), attempt)
        if attempt == 2 or wait > 120:
            raise RuntimeError(f"Source temporarily unavailable: {url} (HTTP {response.status_code})")
        time.sleep(wait)
    raise RuntimeError(f"No response from {url}")


def lookup_pages(session, imdb_ids):
    values = " ".join(json.dumps(imdb_id) for imdb_id in imdb_ids)
    query = f"""
SELECT ?imdb ?item ?title WHERE {{
  VALUES ?imdb {{ {values} }}
  ?item wdt:P345 ?imdb .
  OPTIONAL {{
    ?article schema:about ?item ;
             schema:isPartOf <https://en.wikipedia.org/> ;
             schema:name ?title .
  }}
}}
"""
    result = get_json(session, WIKIDATA_API, {"query": query, "format": "json"})
    pages = {imdb_id: set() for imdb_id in imdb_ids}
    for row in result["results"]["bindings"]:
        imdb_id = row["imdb"]["value"]
        qid = row["item"]["value"].rsplit("/", 1)[-1]
        title = row.get("title", {}).get("value")
        pages[imdb_id].add((qid, title))
    return pages


def parse_page(session, **params):
    result = get_json(
        session,
        f"{WIKIPEDIA}/w/api.php",
        {"action": "parse", "format": "json", "formatversion": 2, "maxlag": 5, **params},
    )
    if "parse" in result:
        return result["parse"]
    error = result.get("error", {})
    if error.get("code") == "missingtitle":
        return None
    raise RuntimeError(f"Wikipedia API error: {error}")


def collect_page(session, imdb_id, options):
    record = {
        "imdb_id": imdb_id,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if not options:
        return {**record, "missing_reason": "no_wikidata_item"}
    if len(options) != 1:
        return {**record, "missing_reason": "ambiguous_wikidata_match"}
    qid, title = next(iter(options))
    record["wikidata_url"] = f"https://www.wikidata.org/wiki/{qid}"
    if not title:
        return {**record, "missing_reason": "no_english_wikipedia_article"}
    person = imdb_id.startswith("nm")
    props = {"prop": "text|revid", "section": 0} if person else {"prop": "tocdata|revid"}
    page = parse_page(session, page=title, redirects=1, **props)
    if page is None:
        return {**record, "page_title": title, "missing_reason": "article_missing"}
    record.update(
        page_title=page["title"],
        source_url=f"{WIKIPEDIA}/w/index.php?oldid={page['revid']}",
    )
    if person:
        return {**record, "section": "Lead", "html": page["text"]}

    sections = page.get("tocdata", {}).get("sections", [])
    plot = next((
        section for section in sections
        if section["line"].casefold() in {"plot", "synopsis", "plot summary"}
    ), None)
    if plot is None:
        return {**record, "missing_reason": "plot_not_found"}
    fragment = parse_page(session, oldid=page["revid"], section=plot["index"], prop="text")
    if fragment is None:
        return {**record, "missing_reason": "article_missing"}
    return {**record, "section": plot["line"], "html": fragment["text"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, default=Path("candidates.csv"))
    parser.add_argument("--cache", type=Path, default=Path("work/raw"))
    parser.add_argument("--user-agent", default=os.getenv(
        "WIKIMEDIA_USER_AGENT",
        "ECE-BigData-IMDb-Pilot/0.1 (https://github.com/benjaminlabos/ece-big-data-processing)",
    ))
    args = parser.parse_args()
    with args.candidates.open(encoding="utf-8-sig", newline="") as stream:
        ids = {row[column] for row in csv.DictReader(stream) for column in ("tconst", "nconst")}
    # The candidate contract uses IMDb IDs. Never turn arbitrary CSV values into filenames.
    if any(
        not imdb_id or imdb_id[:2] not in {"tt", "nm"}
        or not imdb_id[2:].isascii() or not imdb_id[2:].isdigit()
        for imdb_id in ids
    ):
        parser.error("Expected IMDb IDs in the tconst and nconst columns.")
    args.cache.mkdir(parents=True, exist_ok=True)
    pending = iter(sorted(imdb_id for imdb_id in ids if not (args.cache / f"{imdb_id}.json").exists()))
    with requests.Session() as session:
        session.headers.update({"User-Agent": args.user_agent})
        while batch := list(islice(pending, 25)):
            for imdb_id, options in lookup_pages(session, batch).items():
                record = collect_page(session, imdb_id, options)
                path = args.cache / f"{imdb_id}.json"
                temporary = path.with_suffix(".tmp")
                temporary.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
                temporary.replace(path)
                print(f"{imdb_id}\t{record.get('missing_reason') or record['source_url']}", flush=True)
    print(f"Cache ready for {len(ids)} IMDb IDs in {args.cache}", flush=True)


if __name__ == "__main__":
    main()
