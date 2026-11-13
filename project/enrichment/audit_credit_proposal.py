"""Verify and describe a proposed expansion using only an existing frozen cache."""

import argparse
from collections import Counter, defaultdict
import csv
from hashlib import sha256
import json
from pathlib import Path

import pyarrow.parquet as pq


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot_dir", type=Path)
    parser.add_argument("proposal", type=Path)
    parser.add_argument("credit_evidence", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    root = args.snapshot_dir
    requested_rows = read_csv(args.proposal)
    pairs = {(row["tconst"], row["nconst"]) for row in requested_rows}
    assert len(pairs) == len(requested_rows)
    proof = json.loads((args.credit_evidence / "manifest.json").read_bytes())
    credits_file = args.credit_evidence / "credits.csv"
    assert sha256(credits_file.read_bytes()).hexdigest() == proof["credits_sha256"]
    assert sha256(args.proposal.read_bytes().replace(b"\r\n", b"\n")).hexdigest() == proof["requested_pairs_sha256_lf"]
    credits = defaultdict(list)
    for row in read_csv(credits_file):
        credits[row["tconst"], row["nconst"]].append((int(row["ordering"]), row["category"]))
    assert set(credits) == pairs and len(pairs) == proof["matched_pairs"]
    assert sum(map(len, credits.values())) == proof["credit_rows"]

    person_ids = {person for _, person in pairs}
    inputs = root / "inputs"
    primary = {(row["tconst"], row["nconst"]) for row in read_csv(inputs / "primary.csv")}
    reserves = {(row["tconst"], row["nconst"]) for row in read_csv(inputs / "reserves.csv")}
    bounded = {(row["tconst"], row["nconst"]) for row in read_csv(inputs / "enrichment-input.csv")}
    assert primary | reserves == bounded and not (primary & reserves)
    assert not (pairs & bounded)
    results = root / "coverage"
    coverage = {r["tconst"]: r for r in pq.read_table(results / "coverage").to_pylist()}
    coverage_manifest_path = results / "_MANIFEST.json"
    coverage_manifest = json.loads(coverage_manifest_path.read_bytes())
    assert len(coverage) == coverage_manifest["films_requested"] == 6256
    for key, name in (("candidates", "enrichment-input.csv"), ("priority", "primary.csv")):
        assert sha256((inputs / name).read_bytes()).hexdigest() == coverage_manifest["input_sha256"][key]
    detail = pq.read_table(results / "principals", filters=[("nconst", "in", sorted(person_ids))])
    biographies = {}
    for row in detail.to_pylist():
        assert row["missing_reason"] is None and row["text"].strip()
        person = row["nconst"]
        metadata = {field: row[field] for field in ("source_url", "wikidata_url", "license_url", "retrieved_at_utc")}
        metadata["biography_chars"] = len(row["text"])
        assert biographies.setdefault(person, metadata) == metadata
    assert set(biographies) == person_ids

    receipt = json.loads((root / "cache-snapshot.receipt.json").read_bytes())
    digest = sha256()
    cached = {}
    with (root / "cache-snapshot.json").open("rb") as stream:
        for line_number, line in enumerate(stream, start=1):
            digest.update(line)
            record = json.loads(line)
            person = record["imdb_id"]
            if person not in person_ids:
                continue
            assert person not in cached
            assert not record.get("missing_reason") and record["html"].strip()
            for field in ("source_url", "wikidata_url", "retrieved_at_utc"):
                assert biographies[person][field] == record[field]
            assert record["source_url"].startswith("https://en.wikipedia.org/w/index.php?oldid=")
            cached[person] = line_number
    assert digest.hexdigest() == receipt["sha256"] and line_number == receipt["cache_records"]
    assert set(cached) == person_ids

    additions = Counter(film for film, _ in pairs)
    before_three = sum(r["at_least_three_biographies"] for r in coverage.values())
    before_both = sum(r["at_least_three_biographies"] and r["synopsis_available"] for r in coverage.values())
    by_category = Counter()
    by_category_on_crossing = Counter()
    gained, gained_both = set(), set()
    restricted_additions = Counter()
    excluded_categories = {"self", "archive_footage", "archive_sound"}
    rows = []
    for title, person in sorted(pairs):
        film = coverage[title]
        before = film["biographies_available"]
        after = before + additions[title]
        assert before < 3 and after <= 3
        crosses = before < 3 <= after
        synopsis = film["synopsis_available"]
        crossing_both = crosses and synopsis
        ordering, category = min(credits[title, person])
        if any(role not in excluded_categories for _, role in credits[title, person]):
            restricted_additions[title] += 1
        by_category[category] += 1
        if crosses:
            gained.add(title)
            by_category_on_crossing[category] += 1
        if crossing_both:
            gained_both.add(title)
        rows.append({
            "tconst": title,
            "nconst": person,
            "category": category,
            "ordering": ordering,
            "all_verified_credit_rows": json.dumps(sorted(credits[title, person]), ensure_ascii=False),
            "imdb_source_run": proof["prepared_source_run"],
            "imdb_manifest_sha256": proof["prepared_source_sha256"]["input_manifest.json"],
            "current_selection_position": "outside_bounded_selection",
            "biography_available": True,
            "language": "en",
            **biographies[person],
            "frozen_cache_record": f"cache-snapshot.json:{cached[person]}",
            "frozen_cache_sha256": receipt["sha256"],
            "biographies_before": before,
            "biographies_after": after,
            "synopsis_available": synopsis,
            "pair_contributes_to_three": crosses,
            "pair_contributes_to_synopsis_and_three": crossing_both,
        })

    args.output.mkdir(parents=True)
    csv_path = args.output / "proposed-credits.csv"
    with csv_path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    restricted_gains = {
        title for title, extra in restricted_additions.items()
        if coverage[title]["biographies_available"] + extra >= 3
    }
    manifest = {
        "applied": False,
        "new_wikimedia_requests": 0,
        "snapshot": receipt,
        "spark_manifest_sha256": sha256(coverage_manifest_path.read_bytes()).hexdigest(),
        "exact_imdb_credit_projection": proof,
        "proposal_sha256_lf": sha256(csv_path.read_bytes()).hexdigest(),
        "distinct_film_person_pairs": len(rows),
        "distinct_people": len(person_ids),
        "distinct_films_with_proposed_credits": len(additions),
        "additional_credits_by_category": dict(sorted(by_category.items())),
        "threshold_contributions_by_category": dict(sorted(by_category_on_crossing.items())),
        "maximum_extra_cached_credits_for_a_film": max(additions.values()),
        "films_with_three_biographies": {"before": before_three, "after": before_three + len(gained), "gain": len(gained)},
        "films_with_synopsis_and_three": {"before": before_both, "after": before_both + len(gained_both), "gain": len(gained_both)},
        "fixed_proposal_without_self_or_archive": {
            "excluded_categories": sorted(excluded_categories),
            "eligible_proposed_pairs": sum(restricted_additions.values()),
            "gain_three_biographies": len(restricted_gains),
            "gain_synopsis_and_three": sum(coverage[title]["synopsis_available"] for title in restricted_gains),
        },
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
