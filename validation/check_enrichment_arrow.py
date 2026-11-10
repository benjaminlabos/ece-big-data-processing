"""Check Ben's actual HTML extraction UDF with Arrow on synthetic rows only."""

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enrichment-module", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    spec = importlib.util.spec_from_file_location(
        "ben_enrichment_arrow_check", args.enrichment_module
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("Enrichment Arrow compatibility")
        .config("spark.sql.execution.pythonUDF.arrow.enabled", True)
        .config("spark.sql.execution.arrow.pyspark.enabled", True)
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", False)
        .config("spark.sql.shuffle.partitions", 2)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = spark.createDataFrame(
            [
                (
                    1,
                    "Plot",
                    '<div class="mw-parser-output"><p>Synthetic plot.<sup class="reference">[1]</sup></p></div>',
                ),
                (
                    2,
                    "Lead",
                    '<div class="mw-parser-output"><p>Synthetic lead one.</p><p>Synthetic lead two.</p><p>Excluded paragraph.</p></div>',
                ),
                (3, "Lead", None),
            ],
            "row_id long, section string, html string",
        )
        udf = F.udf(module.extract_text, StringType())
        assert udf.evalType == 101, (
            "Expected SQL_ARROW_BATCHED_UDF; fallback is not accepted"
        )
        parsed = rows.withColumn("text", udf("section", "html")).select(
            "row_id", "text"
        )
        expected = {
            1: "Synthetic plot.",
            2: "Synthetic lead one.\n\nSynthetic lead two.",
            3: None,
        }
        assert {row.row_id: row.text for row in parsed.collect()} == expected
        assert (
            "ArrowEvalPython" in parsed._jdf.queryExecution().executedPlan().toString()
        )
        pandas_rows = parsed.orderBy("row_id").toPandas()
        assert pandas_rows["text"].tolist() == [expected[i] for i in (1, 2, 3)]
        result = {
            "scope": "synthetic_arrow_compatibility_only",
            "enrichment_source_commit": args.source_commit,
            "enrichment_module_sha256": hashlib.sha256(
                args.enrichment_module.read_bytes()
            ).hexdigest(),
            "spark_version": spark.version,
            "pandas_version": importlib.metadata.version("pandas"),
            "pyarrow_version": importlib.metadata.version("pyarrow"),
            "beautifulsoup4_version": importlib.metadata.version("beautifulsoup4"),
            "udf_eval_type": udf.evalType,
            "rows_verified": len(expected),
            "arrow_udf_verified": True,
            "arrow_to_pandas_verified_without_fallback": True,
            "live_collection_repeated": False,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
