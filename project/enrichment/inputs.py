"""Input contracts shared by the collector and Spark job."""

import csv
import re


CANDIDATE_COLUMNS = ("tconst", "nconst")
TITLE_COLUMNS = ("tconst",)
ID_PATTERNS = {"tconst": r"tt[0-9]{7,}", "nconst": r"nm[0-9]{7,}"}


def read_ids(path, columns):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        try:
            if tuple(next(reader, ())) != columns:
                raise ValueError(f"{path}: expected CSV header {','.join(columns)}")
            for row in reader:
                if len(row) != len(columns) or any(
                    re.fullmatch(ID_PATTERNS[column], value) is None
                    for column, value in zip(columns, row)
                ):
                    raise ValueError(f"{path}:{reader.line_num}: invalid IMDb IDs for {','.join(columns)}")
                yield tuple(row)
        except csv.Error as error:
            raise ValueError(f"{path}:{reader.line_num}: malformed CSV: {error}") from error
