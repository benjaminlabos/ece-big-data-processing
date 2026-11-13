"""IMDb source columns, keys and types (no inference from samples)."""

from pyspark.sql.types import StringType, StructField, StructType

TABLES = {
    "title.basics": "tconst titleType primaryTitle originalTitle isAdult startYear endYear runtimeMinutes genres".split(),
    "name.basics": "nconst primaryName birthYear deathYear primaryProfession knownForTitles".split(),
    "title.akas": "titleId ordering title region language types attributes isOriginalTitle".split(),
    "title.crew": "tconst directors writers".split(),
    "title.episode": "tconst parentTconst seasonNumber episodeNumber".split(),
    "title.principals": "tconst ordering nconst category job characters".split(),
    "title.ratings": "tconst averageRating numVotes".split(),
}
KEYS = {table: [columns[0]] for table, columns in TABLES.items()}
KEYS["title.akas"] = ["titleId", "ordering"]
KEYS["title.principals"] = ["tconst", "ordering"]

INTEGERS = {
    "title.basics": ["isAdult", "startYear", "endYear", "runtimeMinutes"],
    "name.basics": ["birthYear", "deathYear"],
    "title.akas": ["ordering", "isOriginalTitle"],
    "title.crew": [],
    "title.episode": ["seasonNumber", "episodeNumber"],
    "title.principals": ["ordering"],
    "title.ratings": ["numVotes"],
}
ARRAYS = {
    "title.basics": {"genres": ","},
    "name.basics": {"primaryProfession": ",", "knownForTitles": ","},
    "title.akas": {"types": "\u0002", "attributes": "\u0002"},
    "title.crew": {"directors": ",", "writers": ","},
}
# (column, parent table, parent key, is array)
REFERENCES = {
    "title.akas": [("titleId", "title.basics", "tconst", False)],
    "title.crew": [
        ("tconst", "title.basics", "tconst", False),
        ("directors", "name.basics", "nconst", True),
        ("writers", "name.basics", "nconst", True),
    ],
    "title.episode": [
        ("tconst", "title.basics", "tconst", False),
        ("parentTconst", "title.basics", "tconst", False),
    ],
    "title.principals": [
        ("tconst", "title.basics", "tconst", False),
        ("nconst", "name.basics", "nconst", False),
    ],
    "title.ratings": [("tconst", "title.basics", "tconst", False)],
    "name.basics": [("knownForTitles", "title.basics", "tconst", True)],
}


def raw_schema(table):
    return StructType([StructField(c, StringType(), True) for c in TABLES[table]])
