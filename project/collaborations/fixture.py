"""Build the small collaboration fixture or verify its analysis summary."""

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession


# film, year, director, performer, rating, votes
MOVIES = [
    (1, 2000, 11, 21, 6.0, 100),
    (2, 2000, 11, 21, 8.0, 1000),
    (3, 2001, 11, 21, 9.0, 1000),
    (4, 2002, 11, 21, 7.0, 1000),
    (5, None, 11, 21, 10.0, 5000),
    (6, 2026, 11, 21, 10.0, 5000),
    (7, 2000, 12, 22, 4.0, 99),
    (8, 2001, 12, 22, 6.0, 1000),
    (9, 1990, 13, 23, None, None),
    (10, 1991, 13, 23, 9.0, 1000),
    (11, 1992, 13, 23, 9.0, 1000),
    (12, 1970, 14, 24, 4.0, 1000),
    (13, 1980, 14, 24, 10.0, 1000),
]


def title(number):
    return f"tt{number:07d}"


def person(number):
    return f"nm{number:07d}"


def create(clean_dir):
    clean_dir.mkdir(parents=True, exist_ok=False)
    titles = [(title(i), "movie", year) for i, year, *_ in MOVIES]
    titles.append((title(14), "tvSeries", 1999))
    crew = [(title(i), [person(d)]) for i, _, d, *_ in MOVIES]
    crew[0][1].append(person(11))  # duplicate director in the same movie
    crew.append((title(14), [person(12)]))
    principals = [(title(i), person(p), "actor") for i, _, _, p, *_ in MOVIES]
    principals.extend([
        (title(1), person(21), "actress"),  # two credits for the same pair-film
        (title(1), person(11), "actor"),
        (title(3), person(11), "actor"),  # self-pair has first and later films
        (title(8), person(31), "writer"),
        (title(14), person(22), "actress"),
    ])
    ratings = [
        (title(i), rating, votes)
        for i, _, _, _, rating, votes in MOVIES
        if rating is not None
    ]
    ratings.append((title(14), 9.0, 1000))
    specs = {
        "title.basics": (titles, "tconst STRING, titleType STRING, startYear INT"),
        "title.crew": (crew, "tconst STRING, directors ARRAY<STRING>"),
        "title.principals": (principals, "tconst STRING, nconst STRING, category STRING"),
        "title.ratings": (ratings, "tconst STRING, averageRating DOUBLE, numVotes INT"),
    }
    spark = SparkSession.builder.appName("IMDb collaboration fixture").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        for name, (rows, schema) in specs.items():
            spark.createDataFrame(rows, schema).write.parquet(str(clean_dir / name))
    finally:
        spark.stop()


def verify(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["scope"] == "fixture"
    assert data["include_self"] is False
    assert data["diagnostics"] == {
        "movie_pair_films_before_self_policy": 14,
        "self_pair_films": 2,
        "pair_films_unknown_year": 1,
        "pair_films_in_period_without_rating": 1,
        "first_year_pair_films_in_period_before_vote_filter": 4,
        "later_year_pair_films_in_period_before_vote_filter": 6,
    }
    metrics = (
        "pairs", "first_year_pair_films", "later_year_pair_films",
        "mean_first_year_rating_per_pair", "mean_later_year_rating_per_pair",
        "mean_within_pair_change",
    )
    actual = {
        (row["min_votes"], row["min_history_years"]): tuple(row[k] for k in metrics)
        for row in data["comparisons"]
    }
    assert len(data["comparisons"]) == len(actual)
    assert set(actual) == {(v, y) for v in (0, 100, 1000) for y in (2, 3, 4)}
    expected = {
        (0, 2): (2, 3, 3, 5.5, 7.0, 1.5),
        (0, 3): (1, 2, 2, 7.0, 8.0, 1.0),
        (100, 2): (1, 2, 2, 7.0, 8.0, 1.0),
        (100, 3): (1, 2, 2, 7.0, 8.0, 1.0),
        (1000, 2): (1, 1, 2, 8.0, 8.0, 0.0),
        (1000, 3): (1, 1, 2, 8.0, 8.0, 0.0),
    }
    for key, wanted in expected.items():
        assert actual[key][:3] == wanted[:3], key
        for seen, value in zip(actual[key][3:], wanted[3:]):
            assert math.isclose(seen, value, abs_tol=1e-9), (key, seen, value)
    for votes in (0, 100, 1000):
        assert actual[votes, 4] == (0, 0, 0, None, None, None)
    print(f"Fixture verified under Spark {data['spark_version']}: nine comparisons")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--create", type=Path, metavar="CLEAN_DIR")
    mode.add_argument("--verify", type=Path, metavar="SUMMARY_JSON")
    args = parser.parse_args()
    if args.create:
        create(args.create)
    else:
        verify(args.verify)
