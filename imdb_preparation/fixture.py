"""Controlled seven-table input for the executable smoke test, never IMDb data."""

import argparse
import json
from pathlib import Path

from .download import sha256
from .schema import TABLES

MISSING = "\\N"


def fixture_rows():
    title = [
        "tt0000001",
        "movie",
        'A title with "quotes"',
        "Original",
        "0",
        "2000",
        MISSING,
        "100",
        "Drama,Comedy",
    ]
    return {
        "title.basics": [
            title,
            title.copy(),
            [
                "tt0000002",
                "tvSeries",
                "Series",
                "Series",
                "2",
                "2005",
                "2000",
                "0",
                "Drama",
            ],
            [
                "tt0000003",
                "tvEpisode",
                "Episode",
                "Episode",
                "0",
                "2006",
                MISSING,
                "45",
                "Drama",
            ],
            [
                "tt0000004",
                "movie",
                "Conflict A",
                "A",
                "0",
                "2000",
                MISSING,
                "90",
                "Drama",
            ],
            [
                "tt0000004",
                "movie",
                "Conflict B",
                "B",
                "0",
                "2000",
                MISSING,
                "90",
                "Drama",
            ],
            [
                "bad-id",
                "movie",
                "Bad key",
                "Bad key",
                "0",
                "2000",
                MISSING,
                "90",
                "Drama",
            ],
            [
                "tt0000005",
                "tvEpisode",
                "",
                "   ",
                "0",
                MISSING,
                MISSING,
                "oops",
                "Drama,,Drama",
            ],
            [
                "tt0000006",
                "movie",
                "Future release",
                "Future release",
                "0",
                "2030",
                MISSING,
                "120",
                "Sci-Fi",
            ],
        ],
        "name.basics": [
            [
                "nm0000001",
                "Person One",
                "1980",
                "1970",
                "actor,director",
                "tt0000001,tt9999999,tt0000001",
            ],
            ["nm0000002", "Person Two", "2030", MISSING, "writer", "tt0000002"],
            ["nm0000003", "Person Three", "1900", "2000", MISSING, MISSING],
        ],
        "title.akas": [
            [
                "tt0000001",
                "1",
                'Titre "cité"',
                "FR",
                "fr",
                "alternative\u0002festival",
                MISSING,
                "2",
            ],
            ["tt9999999", "1", "Orphan", "US", "en", MISSING, MISSING, "1"],
            ["tt0000002", "0", "Bad ordering", "US", "en", MISSING, MISSING, "1"],
            ["tt0000003", "01", "Conflict", "US", "en", MISSING, MISSING, "1"],
            ["tt0000003", "1", "Conflict", "US", "en", MISSING, MISSING, "1"],
        ],
        "title.crew": [
            ["tt0000001", "nm0000001,nm9999999,nm0000001", MISSING],
            ["tt0000002", MISSING, "nm0000002"],
            ["tt9999999", "nm0000001", MISSING],
            ["tt0000003", "oops", MISSING],
        ],
        "title.episode": [
            ["tt0000003", "tt0000002", "0", "1"],
            ["tt0000005", "tt9999999", "-1", "two"],
        ],
        "title.principals": [
            ["tt0000001", "1", "nm0000001", "actor", MISSING, '["Hero"]'],
            ["tt0000002", "1", "nm9999999", "actor", " ", "[]"],
            ["tt9999999", "1", "nm0000001", "actor", MISSING, MISSING],
            ["tt0000003", "x", "nm0000001", "actor", MISSING, MISSING],
        ],
        "title.ratings": [
            ["tt0000001", "8.5", "100"],
            ["tt0000002", "11", "-5"],
            ["tt0000003", "NaN", "1.5"],
            ["tt9999999", "7.0", "3"],
            ["tt0000004", "9.0", "10"],
        ],
    }


def write_fixture(destination):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {
        "scope": "fixture",
        "description": "Synthetic controlled test, not IMDb",
        "files": {},
    }
    for table, rows in fixture_rows().items():
        path = destination / (table + ".tsv")
        text = "\t".join(TABLES[table]) + "\n"
        text += "".join("\t".join(row) + "\n" for row in rows)
        path.write_text(text, encoding="utf-8")
        manifest["files"][table] = {
            "path": path.name,
            "sha256": sha256(path),
            "rows": len(rows),
        }
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    write_fixture(args.destination)


if __name__ == "__main__":
    main()
