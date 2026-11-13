"""Reconcile Spark coverage against the CSV inputs and optional frozen cache."""

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import re

import pyarrow.parquet as pq


def rows(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


PROVENANCE_FIELDS = ("page_title", "section", "source_url", "wikidata_url", "retrieved_at_utc")


def audit_cache(snapshot, receipt_path, outputs):
    receipt = json.loads(receipt_path.read_bytes())
    digest = sha256()
    seen = set()
    statuses = Counter()
    with snapshot.open("rb") as stream:
        for line in stream:
            digest.update(line)
            record = json.loads(line)
            identifier = record["imdb_id"]
            assert re.fullmatch(r"(tt|nm)[0-9]{7,}", identifier) and identifier not in seen
            assert datetime.fromisoformat(record["retrieved_at_utc"]).utcoffset() is not None
            seen.add(identifier)
            reason = record.get("missing_reason")
            if not reason:
                assert all(record.get(field) for field in ("html", "section", "source_url", "wikidata_url"))
            if identifier in outputs:
                output = outputs[identifier]
                assert all(record.get(field) == output[field] for field in PROVENANCE_FIELDS), identifier
                expected_reason = "plot_not_found" if identifier.startswith("tt") else "biography_not_found"
                assert output["missing_reason"] == (reason or (None if output["text"] else expected_reason))
                statuses[reason or "source_html"] += 1
    assert len(seen) == receipt["cache_records"] and digest.hexdigest() == receipt["sha256"]
    uncollected = outputs.keys() - seen
    assert uncollected == {
        identifier for identifier, row in outputs.items() if row["missing_reason"] == "not_collected"
    }
    return {
        **receipt,
        "requested_ids": len(outputs),
        "responses_in_selection": sum(statuses.values()),
        "documented_absences_before_extraction": sum(statuses.values()) - statuses["source_html"],
        "preserved_responses_outside_selection": len(seen - outputs.keys()),
        "requested_ids_still_missing": len(uncollected),
        "statuses_before_extraction": dict(statuses),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("titles", type=Path)
    parser.add_argument("candidates", type=Path)
    parser.add_argument("primary", type=Path)
    parser.add_argument("top", type=Path)
    parser.add_argument("--snapshot", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--biography-gaps", type=Path)
    args = parser.parse_args()
    if bool(args.snapshot) != bool(args.receipt):
        parser.error("--snapshot and --receipt must be provided together.")
    output = args.output
    manifest = json.loads((output / "_MANIFEST.json").read_text(encoding="utf-8"))
    assert (output / "_COMPLETE").exists() == manifest["collection_complete"]
    if args.require_complete:
        assert manifest["collection_complete"], "Collection has pending identifiers."
    input_sha256 = {
        name: sha256(path.read_bytes()).hexdigest()
        for name, path in (("titles", args.titles), ("candidates", args.candidates),
                           ("priority", args.primary), ("top", args.top))
    }
    assert manifest["input_sha256"] == input_sha256
    tables = {key: pq.read_table(output / key).to_pylist()
              for key in ("films", "principals", "coverage", "coverage_by_year_genre")}
    titles = {r["tconst"] for r in rows(args.titles)}
    candidates = {(r["tconst"], r["nconst"]) for r in rows(args.candidates)}
    priority = {(r["tconst"], r["nconst"]) for r in rows(args.primary)}
    assert {r["tconst"] for r in tables["films"]} == titles
    assert {(r["tconst"], r["nconst"]) for r in tables["principals"]} == candidates
    assert len(tables["films"]) == manifest["films_requested"] == len(titles)
    assert len(tables["principals"]) == manifest["candidate_pairs"] == len(candidates)

    credits = defaultdict(list)
    people = {}
    for r in tables["principals"]:
        credits[r["tconst"]].append(r)
        person = {k: v for k, v in r.items() if k not in {"tconst", "is_primary"}}
        assert people.setdefault(r["nconst"], person) == person
        assert r["is_primary"] == ((r["tconst"], r["nconst"]) in priority)
    assert len(people) == manifest["people_requested"]
    assert len(tables["coverage"]) == len(titles)
    coverage = {r["tconst"]: r for r in tables["coverage"]}
    for film in tables["films"]:
        key = film["tconst"]
        metric = coverage[key]
        people_with_bio = [r for r in credits[key] if r["missing_reason"] is None]
        assert metric["candidate_people"] == len(credits[key])
        assert metric["biographies_available"] == len(people_with_bio)
        assert metric["primary_biographies_available"] == sum(r["is_primary"] for r in people_with_bio)
        assert metric["at_least_three_biographies"] == (len(people_with_bio) >= 3)
        assert metric["fewer_than_three_candidates"] == (len(credits[key]) < 3)
        assert metric["synopsis_available"] == (film["missing_reason"] is None)
        assert metric["synopsis_missing_reason"] == film["missing_reason"]

    groups = defaultdict(set)
    for r in rows(args.top):
        groups[(int(r["startYear"]), r["genre"])].add(r["tconst"])
    assert len(tables["coverage_by_year_genre"]) == len(groups)
    assert {(r["startYear"], r["genre"]) for r in tables["coverage_by_year_genre"]} == set(groups)
    for r in tables["coverage_by_year_genre"]:
        group = groups[(r["startYear"], r["genre"])]
        assert r["films_requested"] == len(group)
        assert r["synopses_available"] == sum(coverage[key]["synopsis_available"] for key in group)
        assert r["films_with_three_biographies"] == sum(coverage[key]["at_least_three_biographies"] for key in group)
        assert r["films_with_fewer_than_three_candidates"] == sum(
            coverage[key]["fewer_than_three_candidates"] for key in group
        )
    for statuses, records in (
        ("films_by_status", tables["films"]), ("people_by_status", people.values()),
        ("credits_by_status", tables["principals"]),
    ):
        assert manifest[statuses] == Counter(r["missing_reason"] or "available" for r in records)
    assert manifest["films_with_three_biographies"] == sum(
        r["at_least_three_biographies"] for r in coverage.values()
    )
    assert manifest["films_with_fewer_than_three_candidates"] == sum(
        r["fewer_than_three_candidates"] for r in coverage.values()
    )
    assert manifest["films_reaching_three_with_reserves"] == sum(
        r["at_least_three_biographies"] and r["primary_biographies_available"] < 3
        for r in coverage.values()
    )
    examples = {}
    for kind, records in (("films", tables["films"]), ("people", list(people.values()))):
        examples[kind] = []
        for r in sorted(records, key=lambda row: row.get("tconst") or row["nconst"]):
            key = r.get("tconst") or r["nconst"]
            if r["missing_reason"]:
                assert not r["text"]
                continue
            assert r["text"] and r["section"]
            assert re.fullmatch(r"https://en\.wikipedia\.org/w/index\.php\?oldid=[0-9]+", r["source_url"])
            assert re.fullmatch(r"https://www\.wikidata\.org/wiki/Q[0-9]+", r["wikidata_url"])
            assert r["license_url"] == "https://creativecommons.org/licenses/by-sa/4.0/"
            assert datetime.fromisoformat(r["retrieved_at_utc"]).utcoffset().total_seconds() == 0
            if len(examples[kind]) < 3:
                examples[kind].append({k: r[k] for k in (*PROVENANCE_FIELDS, "license_url")
                } | {"imdb_id": key, "text_characters": len(r["text"])})
    films_incomplete = Counter()
    biography_gaps = []
    for r in sorted(coverage.values(), key=lambda row: row["tconst"]):
        synopsis, biographies = r["synopsis_available"], r["at_least_three_biographies"]
        if not synopsis or not biographies:
            label = "synopsis_and_biographies" if not synopsis and not biographies else (
                "synopsis" if not synopsis else "biographies"
            )
            films_incomplete[label] += 1
        if not biographies:
            biography_gaps.append({
                "tconst": r["tconst"],
                "biographies_available": r["biographies_available"],
                "missing_biographies": 3 - r["biographies_available"],
                "candidate_people": r["candidate_people"],
                "fewer_than_three_candidates": r["fewer_than_three_candidates"],
                "biography_missing_reasons": json.dumps(
                    Counter(credit["missing_reason"] for credit in credits[r["tconst"]] if credit["missing_reason"]),
                    sort_keys=True,
                ),
            })
    proof = {
        "manifest_sha256": sha256((output / "_MANIFEST.json").read_bytes()).hexdigest(),
        "verified_parquet_rows": {k: len(v) for k, v in tables.items()},
        "films_with_synopsis_and_three_biographies": len(coverage) - sum(films_incomplete.values()),
        "films_missing_requirements": dict(films_incomplete),
        "structural_shortages": sorted(
            r["tconst"] for r in coverage.values() if r["fewer_than_three_candidates"]
        ),
        "groups_with_at_least_one_synopsis": sum(
            r["synopses_available"] > 0 for r in tables["coverage_by_year_genre"]
        ),
        "groups_with_at_least_one_film_with_three_biographies": sum(
            r["films_with_three_biographies"] > 0 for r in tables["coverage_by_year_genre"]
        ),
        "coverage": manifest,
        "examples": examples,
    }
    if args.snapshot:
        films = {row["tconst"]: row for row in tables["films"]}
        proof["raw_snapshot"] = audit_cache(args.snapshot, args.receipt, films | people)
    if args.biography_gaps:
        columns = ("tconst", "biographies_available", "missing_biographies",
                   "candidate_people", "fewer_than_three_candidates", "biography_missing_reasons")
        with args.biography_gaps.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(biography_gaps)
    print(json.dumps(proof, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
