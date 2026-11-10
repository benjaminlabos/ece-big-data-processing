"""Extract open-source text and join it to IMDb candidates with PySpark."""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

CANDIDATE_SCHEMA = "tconst STRING, nconst STRING"
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


def write_results(spark, candidate_path, raw_path, output):
    candidates = (
        spark.read.schema(CANDIDATE_SCHEMA)
        .option("header", True).option("enforceSchema", False).option("mode", "FAILFAST")
        .csv(candidate_path)
        .distinct()
    )
    invalid = (
        F.col("tconst").isNull() | ~F.col("tconst").rlike(r"^tt[0-9]{7,}$")
        | F.col("nconst").isNull() | ~F.col("nconst").rlike(r"^nm[0-9]{7,}$")
    )
    if candidates.where(invalid).take(1):
        raise ValueError("Invalid tconst/nconst in the candidate CSV.")

    raw = spark.read.schema(RAW_SCHEMA).option("mode", "FAILFAST").json(raw_path)
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
        .cache()
    )

    films = candidates.select("tconst").distinct().join(
        enriched.withColumnRenamed("imdb_id", "tconst"), "tconst", "left"
    )
    principals = candidates.join(
        enriched.withColumnRenamed("imdb_id", "nconst"), "nconst", "left"
    )
    for name, data in (("films", films), ("principals", principals)):
        data = data.withColumn(
            "missing_reason",
            F.when(F.col("retrieved_at_utc").isNull(), F.lit("not_collected"))
            .otherwise(F.col("missing_reason")),
        )
        path = f"{output}/{name}"
        data.write.mode("overwrite").parquet(path)
        saved = spark.read.parquet(path)
        saved.groupBy(F.coalesce("missing_reason", F.lit("available")).alias("status")).count().show()
        columns = ["tconst"] + (["nconst"] if name == "principals" else [])
        saved.select(*columns, "page_title", "section", "source_url", "missing_reason").show(
            n=12, truncate=False
        )
    enriched.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default="candidates.csv")
    parser.add_argument("--raw", default="work/raw/*.json")
    parser.add_argument("--output", default="work/results")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("IMDb Wikipedia enrichment pilot").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        write_results(spark, args.candidates, args.raw, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
