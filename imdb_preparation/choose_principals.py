"""Choose provisional principal and reserve IDs using already verified credit exports."""

import argparse
import json
import os
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

from .download import sha256
from .exports import read_csv, single_csv
from .prepare import json_write


def choose(credits):
    pool = credits.groupBy("tconst", "nconst").agg(
        F.min("ordering").alias("first_order"),
        F.min(F.when(F.col("category") == "director", F.col("ordering"))).alias(
            "director_order"
        ),
        F.min(
            F.when(F.col("category").isin("actor", "actress"), F.col("ordering"))
        ).alias("performer_order"),
        F.sort_array(F.collect_set("category")).alias("roles"),
    )
    directors = (
        pool.filter(F.col("director_order").isNotNull())
        .withColumn(
            "_r",
            F.row_number().over(
                Window.partitionBy("tconst").orderBy("director_order", "nconst")
            ),
        )
        .filter("_r=1")
        .drop("_r")
    )
    flags = directors.select("tconst").withColumn("_has_director", F.lit(1))
    others = (
        pool.join(
            directors.select("tconst", "nconst"), ["tconst", "nconst"], "left_anti"
        )
        .join(flags, "tconst", "left")
        .fillna(0, ["_has_director"])
        .withColumn(
            "_priority", F.when(F.col("performer_order").isNotNull(), 0).otherwise(1)
        )
        .withColumn("_order", F.coalesce("performer_order", "first_order"))
        .withColumn(
            "_r",
            F.row_number().over(
                Window.partitionBy("tconst").orderBy("_priority", "_order", "nconst")
            ),
        )
        .filter(F.col("_r") <= 3 - F.col("_has_director"))
        .select(*pool.columns)
    )
    primary = directors.unionByName(others)
    reserves = (
        pool.join(primary.select("tconst", "nconst"), ["tconst", "nconst"], "left_anti")
        .withColumn(
            "_priority",
            F.when(F.col("director_order").isNotNull(), 0)
            .when(F.col("performer_order").isNotNull(), 1)
            .otherwise(2),
        )
        .withColumn(
            "_order", F.coalesce("director_order", "performer_order", "first_order")
        )
        .withColumn(
            "_r",
            F.row_number().over(
                Window.partitionBy("tconst").orderBy("_priority", "_order", "nconst")
            ),
        )
        .filter("_r <= 2")
        .select(*pool.columns)
    )
    return primary, reserves


def run(spark, source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists():
        raise FileExistsError(output)
    if output == source or source in output.parents or output in source.parents:
        raise ValueError("Separate output required")
    if not (source / "PROVISIONAL_SUCCESS.json").exists():
        raise ValueError("Verified selected-credit export required")
    original = json.loads((source / "summary.json").read_text())
    if original.get("student_validated") is not False or not original["scope"].endswith(
        "_selected_credits_subset"
    ):
        raise ValueError("Provisional selected-credit scope required")
    credits = read_csv(
        spark,
        source / "credit_details.csv",
        ["tconst", "nconst", "ordering", "category"],
    ).withColumn("ordering", F.col("ordering").cast("long"))
    primary, reserves = choose(credits)
    stage = output.with_name(output.name + ".incomplete-" + uuid.uuid4().hex[:8])
    stage.mkdir(parents=True)
    primary.write.parquet(str(stage / "primary"))
    reserves.write.parquet(str(stage / "reserves"))
    primary, reserves = (
        spark.read.parquet(str(stage / part)) for part in ("primary", "reserves")
    )
    all_ids = primary.select("tconst", "nconst").unionByName(
        reserves.select("tconst", "nconst")
    )
    single_csv(primary.select("tconst", "nconst"), stage / "primary.csv")
    single_csv(reserves.select("tconst", "nconst"), stage / "reserves.csv")
    single_csv(all_ids, stage / "enrichment-input.csv")
    if all_ids.count() != all_ids.distinct().count():
        raise AssertionError("A person was selected more than once per film")
    coverage = read_csv(
        spark,
        source / "coverage.csv",
        [
            "tconst",
            "selection_status",
            "principal_rows",
            "candidate_people",
            "rejected_credit_rows",
            "fewer_than_three_candidates",
        ],
    )
    counts = primary.groupBy("tconst").agg(
        F.count("*").alias("primary_people"),
        F.count_if(F.col("director_order").isNotNull()).alias("primary_directors"),
        F.count_if(F.col("performer_order").isNotNull()).alias("primary_performers"),
    )
    coverage = (
        coverage.join(counts, "tconst", "left")
        .join(
            reserves.groupBy("tconst").agg(F.count("*").alias("reserve_people")),
            "tconst",
            "left",
        )
        .fillna(
            0,
            [
                "primary_people",
                "primary_directors",
                "primary_performers",
                "reserve_people",
            ],
        )
    )
    if coverage.filter("primary_people > 3 OR reserve_people > 2").count():
        raise AssertionError("Principal count exceeds the working policy")
    single_csv(coverage, stage / "coverage.csv")
    summary = {
        "scope": original["scope"],
        "student_validated": False,
        "complete_preparation_validated": False,
        "source_summary_sha256": sha256(source / "summary.json"),
        "source_credit_details_sha256": sha256(source / "credit_details.csv"),
        "source_selection_sha256": original["selected_titles_sha256"],
        "module_sha256": sha256(Path(__file__)),
        "spark_version": spark.version,
        "policy": "earliest director credit, then earliest two other performers; fill missing roles with other credits; up to two distinct reserves, directors then performers then other roles",
        "requested_films": coverage.count(),
        "primary_pairs": primary.count(),
        "reserve_pairs": reserves.count(),
        "input_pairs": all_ids.count(),
        "distinct_people_to_lookup": all_ids.select("nconst").distinct().count(),
        "films_with_fewer_than_three_primary_people": coverage.filter(
            "primary_people < 3"
        ).count(),
    }
    json_write(stage / "summary.json", summary)
    json_write(
        stage / "PROVISIONAL_SUCCESS.json",
        {"scope": summary["scope"], "student_validated": False},
    )
    os.rename(stage, output)
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Provisional enrichment input")
        .config("spark.sql.shuffle.partitions", 16)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(run(spark, a.source, a.output), indent=2))
    finally:
        spark.stop()
