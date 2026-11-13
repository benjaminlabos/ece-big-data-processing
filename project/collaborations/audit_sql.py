import argparse
import json
from math import isclose
from pathlib import Path

import duckdb


parser = argparse.ArgumentParser()
parser.add_argument("clean", type=Path)
parser.add_argument("summary", type=Path)
args = parser.parse_args()
summary = json.loads(args.summary.read_text(encoding="utf-8"))
db = duckdb.connect(config={"threads": 4, "memory_limit": "1700MB", "preserve_insertion_order": False})
for name, table in (("titles", "title.basics"), ("crew", "title.crew"),
                    ("credits", "title.principals"), ("ratings", "title.ratings")):
    db.read_parquet(str(args.clean / table / "*.parquet")).create_view(name)
db.execute("""
CREATE TEMP TABLE triples AS
SELECT DISTINCT director, c.nconst AS performer, t.tconst, t.startYear AS year
FROM titles t
JOIN (SELECT tconst, unnest(directors) AS director FROM crew) d USING (tconst)
JOIN credits c USING (tconst)
WHERE t.titleType = 'movie' AND (t.startYear <= ? OR t.startYear IS NULL)
  AND c.category IN ('actor', 'actress') AND director IS NOT NULL AND c.nconst IS NOT NULL
""", [summary["end_year"]])
total, same_person = db.execute(
    "SELECT count(*), count(*) FILTER (WHERE director=performer) FROM triples"
).fetchone()
assert total == summary["diagnostics"]["movie_pair_films_before_self_policy"]
assert same_person == summary["diagnostics"]["self_pair_films"]
condition = "true" if summary["include_self"] else "director <> performer"
db.execute(f"CREATE TEMP VIEW population AS SELECT * FROM triples WHERE {condition}")
db.execute("""
CREATE TEMP TABLE enriched AS
WITH chronology AS (
    SELECT director, performer, min(year) AS first_year, count(DISTINCT year) AS history
    FROM population GROUP BY director, performer
)
SELECT p.*, h.first_year, h.history, r.averageRating AS rating, r.numVotes AS votes
FROM population p JOIN chronology h USING (director, performer)
LEFT JOIN ratings r USING (tconst)
""")
start = summary["start_year"]
diag = db.execute("""
SELECT count(*) FILTER (WHERE year IS NULL),
       count(*) FILTER (WHERE year >= $start AND rating IS NULL),
       count(*) FILTER (WHERE year >= $start AND year = first_year),
       count(*) FILTER (WHERE year >= $start AND year > first_year)
FROM enriched
""", {"start": start}).fetchone()
for key, value in zip((
    "pair_films_unknown_year", "pair_films_in_period_without_rating",
    "first_year_pair_films_in_period_before_vote_filter", "later_year_pair_films_in_period_before_vote_filter",
), diag):
    assert summary["diagnostics"][key] == value, key
db.execute("CREATE TEMP TABLE thresholds(votes_min INT, years_min INT)")
db.executemany("INSERT INTO thresholds VALUES (?, ?)", [
    (row["min_votes"], row["min_history_years"]) for row in summary["comparisons"]
])
actual = db.execute("""
WITH pair_scores AS (
    SELECT votes_min, years_min, director, performer,
           count(*) FILTER (WHERE year = first_year) AS first_films,
           count(*) FILTER (WHERE year > first_year) AS later_films,
           avg(rating) FILTER (WHERE year = first_year) AS first_mean,
           avg(rating) FILTER (WHERE year > first_year) AS later_mean
    FROM enriched CROSS JOIN thresholds
    WHERE year >= ? AND rating IS NOT NULL AND votes >= votes_min AND history >= years_min
    GROUP BY votes_min, years_min, director, performer
    HAVING first_films > 0 AND later_films > 0
)
SELECT votes_min, years_min, count(director),
       coalesce(sum(first_films), 0), coalesce(sum(later_films), 0),
       avg(first_mean), avg(later_mean), avg(later_mean - first_mean)
FROM thresholds LEFT JOIN pair_scores USING (votes_min, years_min)
GROUP BY votes_min, years_min ORDER BY votes_min, years_min
""", [start]).fetchall()
keys = ("min_votes", "min_history_years", "pairs", "first_year_pair_films",
        "later_year_pair_films", "mean_first_year_rating_per_pair",
        "mean_later_year_rating_per_pair", "mean_within_pair_change")
assert len(actual) == len(summary["comparisons"])
for observed, expected in zip(actual, summary["comparisons"]):
    for key, value in zip(keys, observed):
        assert (value == expected[key] if not isinstance(value, float) else
                isclose(value, expected[key], abs_tol=1e-10)), (key, value, expected[key])
print(json.dumps({"duckdb": duckdb.__version__, "scope": summary["scope"],
                  "verified_diagnostics": summary["diagnostics"], "verified_cells": len(actual)}, indent=2))
