"""Extract open-source text and join it to IMDb candidates with PySpark."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

TOP_SCHEMA = """
startYear INT, genre STRING, genre_year_rank INT, tconst STRING, primaryTitle STRING,
runtimeMinutes INT, averageRating DOUBLE, numVotes LONG
"""
RAW_SCHEMA = """
imdb_id STRING, wikidata_url STRING, page_title STRING, source_url STRING,
retrieved_at_utc STRING, section STRING, html STRING, missing_reason STRING
"""
LICENSE_URL = "https://creativecommons.org/licenses/by-sa/4.0/"


def extract_text(section, html):
    from bs4 import BeautifulSoup

    if not html:
        return None
    root = BeautifulSoup(html, "html.parser").select_one(".mw-parser-output")
    if root is None:
        return None
    paragraphs = []
    for paragraph in root.find_all("p", recursive=False):
        for note in paragraph.select("sup.reference, .mw-editsection, .noprint"):
            note.decompose()
        text = " ".join(paragraph.get_text().split())
        if text:
            paragraphs.append(text)
            if section == "Lead" and len(paragraphs) == 2:
                break
    return "\n\n".join(paragraphs) or None


def read_csv(spark, path, schema):
    return (
        spark.read.schema(schema)
        .option("header", True).option("enforceSchema", False).option("mode", "FAILFAST")
        .csv(str(path))
    )


def read_id_csv(spark, path, columns):
    from inputs import read_ids

    # Spark's CSV reader accepts extra fields. Check the same strict contract as collect.py.
    for _ in read_ids(Path(path), columns):
        pass
    return read_csv(spark, path, ", ".join(f"{column} STRING" for column in columns)).distinct()


def read_cache(spark, raw_path):
    raw = (
        spark.read.schema(RAW_SCHEMA).option("mode", "FAILFAST")
        .option("pathGlobFilter", "*.json").json(str(raw_path))
    )
    invalid_key = ~F.coalesce(F.col("imdb_id").rlike(r"^(tt|nm)[0-9]{7,}$"), F.lit(False))
    if raw.where(invalid_key | F.col("retrieved_at_utc").isNull()).take(1):
        raise ValueError("Invalid IMDb ID or collection date in the JSON cache.")
    if raw.groupBy("imdb_id").count().where(F.col("count") > 1).take(1):
        raise ValueError("Multiple cache records for the same IMDb ID.")
    return raw


def with_status(data):
    return data.withColumn(
        "missing_reason",
        F.when(F.col("retrieved_at_utc").isNull(), F.lit("not_collected"))
        .otherwise(F.col("missing_reason")),
    )


def counts(data):
    return {
        row.status: row["count"]
        for row in data.groupBy(F.coalesce("missing_reason", F.lit("available")).alias("status")).count().collect()
    }


def save_table(spark, path, data):
    data.write.parquet(str(path))
    return spark.read.parquet(str(path))


def file_sha256(path):
    digest = sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def write_results(spark, candidate_path, raw_path, output, titles_path=None, primary_path=None, top_path=None):
    from inputs import CANDIDATE_COLUMNS, TITLE_COLUMNS

    candidates = read_id_csv(spark, candidate_path, CANDIDATE_COLUMNS)
    candidate_titles = candidates.select("tconst").distinct()
    titles = read_id_csv(spark, titles_path, TITLE_COLUMNS) if titles_path else candidate_titles
    if candidate_titles.join(titles, "tconst", "left_anti").take(1):
        raise ValueError("Candidate CSV contains a film absent from the selected titles.")
    if not titles.take(1):
        raise ValueError("No films to enrich.")
    keys = list(CANDIDATE_COLUMNS)
    if primary_path:
        primary = read_id_csv(spark, primary_path, CANDIDATE_COLUMNS)
        if primary.join(candidates, keys, "left_anti").take(1):
            raise ValueError("Priority CSV contains a pair absent from the candidates.")
        candidates = candidates.join(primary.withColumn("is_primary", F.lit(True)), keys, "left").fillna(
            False, subset=["is_primary"]
        )

    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f"Choose a new output path: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = read_cache(spark, raw_path)
    parse = F.udf(extract_text, StringType())
    enriched = (
        raw.withColumn("text", parse("section", "html"))
        .withColumn(
            "missing_reason",
            F.coalesce(
                "missing_reason",
                F.when(F.col("source_url").isNull(), F.lit("source_url_missing"))
                .when(
                    F.col("text").isNull(),
                    F.when(F.col("imdb_id").startswith("tt"), F.lit("plot_not_found"))
                    .otherwise(F.lit("biography_not_found")),
                ),
            ),
        )
        .withColumn("license_url", F.when(F.col("source_url").isNotNull(), F.lit(LICENSE_URL)))
        .select(
            "imdb_id", "page_title", "source_url", "wikidata_url", "retrieved_at_utc",
            "license_url", "section", "text", "missing_reason",
        )
    )
    with tempfile.TemporaryDirectory(prefix=f".{output.name}-", dir=output.parent) as staging:
        # Both output joins use the same cache snapshot, even during collection.
        stage = Path(staging)
        results = stage / "results"
        enriched = save_table(spark, stage / "snapshot", enriched)
        films = save_table(spark, results / "films", with_status(
            titles.join(enriched.withColumnRenamed("imdb_id", "tconst"), "tconst", "left")
        ))
        principals = save_table(spark, results / "principals", with_status(
            candidates.join(enriched.withColumnRenamed("imdb_id", "nconst"), "nconst", "left")
        ))
        people = principals.select("nconst", "missing_reason").distinct()
        available = F.col("missing_reason").isNull()
        aggregates = [
            F.count("*").alias("candidate_people"),
            F.count_if(available).alias("biographies_available"),
        ]
        if primary_path:
            aggregates.append(F.count_if(available & F.col("is_primary")).alias("primary_biographies_available"))
        per_film = principals.groupBy("tconst").agg(*aggregates)
        number_columns = [column for column in per_film.columns if column != "tconst"]
        coverage = save_table(spark, results / "coverage", films.select(
            "tconst", available.alias("synopsis_available"), F.col("missing_reason").alias("synopsis_missing_reason")
        ).join(per_film, "tconst", "left").fillna(0, subset=number_columns).withColumns({
            "fewer_than_three_candidates": F.col("candidate_people") < 3,
            "at_least_three_biographies": F.col("biographies_available") >= 3,
        }))
        if top_path:
            memberships = read_csv(spark, top_path, TOP_SCHEMA).select("startYear", "genre", "tconst").distinct()
            invalid = memberships.where(
                F.col("startYear").isNull() | F.col("genre").isNull() | F.col("tconst").isNull()
            )
            if (invalid.take(1) or memberships.join(titles, "tconst", "left_anti").take(1)
                    or titles.join(memberships, "tconst", "left_anti").take(1)):
                raise ValueError("Top CSV memberships and selected films disagree.")
            save_table(spark, results / "coverage_by_year_genre", memberships.join(coverage, "tconst").groupBy(
                "startYear", "genre"
            ).agg(
                F.count("*").alias("films_requested"),
                F.count_if(F.col("synopsis_available")).alias("synopses_available"),
                F.count_if(F.col("at_least_three_biographies")).alias("films_with_three_biographies"),
                F.count_if(F.col("fewer_than_three_candidates")).alias("films_with_fewer_than_three_candidates"),
            ))
        flags = coverage.agg(
            F.count_if(F.col("at_least_three_biographies")).alias("films_with_three_biographies"),
            F.count_if(F.col("fewer_than_three_candidates")).alias("films_with_fewer_than_three_candidates"),
        ).first().asDict()
        if primary_path:
            flags["films_reaching_three_with_reserves"] = coverage.where(
                F.col("at_least_three_biographies") & (F.col("primary_biographies_available") < 3)
            ).count()
        film_status, person_status, credit_status = counts(films), counts(people), counts(principals)
        remaining = film_status.get("not_collected", 0) + person_status.get("not_collected", 0)
        manifest = {
            "spark_version": spark.version,
            "input_sha256": {
                name: file_sha256(path) for name, path in
                (("candidates", candidate_path), ("titles", titles_path), ("priority", primary_path), ("top", top_path))
                if path
            },
            "films_requested": sum(film_status.values()),
            "people_requested": sum(person_status.values()),
            "candidate_pairs": sum(credit_status.values()),
            "films_by_status": film_status,
            "people_by_status": person_status,
            "credits_by_status": credit_status,
            **flags,
            "collection_complete": remaining == 0,
        }
        (results / "_MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        if remaining == 0:
            (results / "_COMPLETE").touch()
        results.rename(output)
        print(json.dumps(manifest, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default="candidates.csv")
    parser.add_argument("--titles", help="Selected films CSV, including films with no candidates")
    parser.add_argument("--primary", help="Optional tconst,nconst CSV of priority candidates")
    parser.add_argument("--top", help="Optional top 10 CSV for coverage by year and genre")
    parser.add_argument("--raw", default="work/raw/*.json")
    parser.add_argument("--output", default="work/results")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("IMDb Wikipedia enrichment").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        write_results(spark, args.candidates, args.raw, args.output, args.titles, args.primary, args.top)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
