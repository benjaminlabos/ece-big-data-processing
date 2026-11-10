"""Run one isolated Wikimedia → Kafka → Spark experiment."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

from pyspark.sql import SparkSession, functions as F


LAB = Path(__file__).resolve().parents[1]
SCHEMA = (
    "title STRING, wiki STRING, server_name STRING, namespace INT, "
    "type STRING, timestamp LONG, bot BOOLEAN, revision STRUCT<new: LONG>, "
    "meta STRUCT<id: STRING, domain: STRING, uri: STRING>"
)


def title_key(title):
    return F.trim(F.regexp_replace(title, "[_ ]+", " "))


def reference_table(spark, path):
    rows = (
        spark.read.option("header", True).option("escape", '"')
        .option("mode", "FAILFAST").csv(path)
    )
    required = {"wiki", "title", "imdb_id", "wikidata_id"}
    if not required.issubset(rows.columns):
        raise ValueError(f"The reference CSV must include {', '.join(sorted(required))}")
    rows = rows.select(
        F.trim("wiki").alias("wiki"),
        title_key(F.col("title")).alias("title_key"),
        F.trim("imdb_id").alias("imdb_id"),
        F.trim("wikidata_id").alias("wikidata_id"),
    )
    valid = (
        F.col("wiki").rlike(r"^[a-z0-9_-]+wiki$")
        & (F.length("title_key") > 0)
        & F.col("imdb_id").rlike(r"^(tt|nm)[0-9]+$")
        & (F.col("wikidata_id").isNull() | F.col("wikidata_id").rlike(r"^Q[0-9]+$"))
    )
    if rows.filter(~F.coalesce(valid, F.lit(False))).take(1):
        raise ValueError("The reference CSV contains an invalid wiki, title or identifier")
    if not rows.take(1):
        raise ValueError("The reference CSV is empty")
    return rows.groupBy("wiki", "title_key").agg(
        F.sort_array(F.collect_set(F.struct("imdb_id", "wikidata_id"))).alias("imdb_matches")
    )


def streaming_events(spark, broker, topic, reference):
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("event"),
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_time"),
        )
        .select(
            "event.title", "event.wiki", "event.server_name", "event.bot",
            F.col("event.meta.id").alias("event_id"),
            F.col("event.meta.uri").alias("page_url"),
            F.col("event.revision.new").alias("revision_id"),
            F.timestamp_seconds("event.timestamp").alias("event_time"),
            F.current_timestamp().alias("processing_time"),
            "partition", "offset", "kafka_time",
            F.coalesce(
                (F.col("event.type") == "edit")
                & (F.col("event.namespace") == 0)
                & F.col("event.server_name").rlike(r"^[a-z0-9-]+\.wikipedia\.org$")
                & (F.col("event.meta.domain") == F.col("event.server_name"))
                & F.col("event.meta.id").isNotNull()
                & F.col("event.timestamp").isNotNull()
                & F.col("event.title").isNotNull(),
                F.lit(False),
            ).alias("eligible"),
        )
    )
    if reference is None:
        return events.withColumn(
            "imdb_matches", F.lit(None).cast("array<struct<imdb_id:string,wikidata_id:string>>")
        )
    return (
        events.withColumn("title_key", F.when(F.col("eligible"), title_key(F.col("title"))))
        .join(reference, ["wiki", "title_key"], "left")
        .drop("title_key")
    )


def collect_evidence(spark, run, producer, capture_seconds, capture_started, topic, duration, reference_path):
    rows = spark.read.parquet(str(run / "events"))
    matched = F.size("imdb_matches") > 0
    counts = rows.agg(
        F.count("*").alias("kafka_records"),
        F.count_distinct("event_id").alias("distinct_event_ids"),
        F.count_if("eligible").alias("eligible_wikipedia_edits"),
        F.count_distinct(F.when(matched, F.col("event_id"))).alias("matched_events"),
        F.count_distinct(F.when(matched, F.struct("wiki", "title"))).alias("matched_pages"),
    ).first().asDict()
    samples = (
        rows.filter(matched if counts["matched_events"] else F.lit(True))
        .orderBy("partition", "offset").limit(10).toJSON().collect()
    )
    audit = run / "capture.jsonl"
    with audit.open(encoding="utf-8") as handle:
        sent = [json.loads(line).get("meta", {}).get("id") for line in handle]
    evidence = {
        "topic": topic,
        "capture_started_utc": capture_started,
        "capture_seconds_requested": duration,
        "capture_seconds_including_flush": round(capture_seconds, 3),
        "spark_version": spark.version,
        "reference_sha256": (
            hashlib.sha256(reference_path.read_bytes()).hexdigest() if reference_path else None
        ),
        "producer": producer,
        **counts,
        "smoke_event_id_verified": (
            counts["kafka_records"] == len(sent) == 1
            and bool(sent[0]) and sent[0] == json.loads(samples[0]).get("event_id")
        ),
        "examples": [json.loads(sample) for sample in samples],
    }
    summary = json.dumps(evidence, ensure_ascii=False, indent=2)
    (run / "summary.json").write_text(summary + "\n", encoding="utf-8")
    print(summary, flush=True)
    if len(sent) != producer["delivered"] or counts["kafka_records"] != producer["delivered"]:
        raise RuntimeError("The local audit, Kafka delivery and Spark counts disagree")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", help="CSV of wiki, title, imdb_id and wikidata_id")
    parser.add_argument("--languages", default="en,fr", help="Comma-separated Wikipedia languages")
    parser.add_argument("--duration", type=int, default=900, help="Capture duration in seconds")
    parser.add_argument("--max-events", type=int, default=0, help="Optional bound for smoke testing")
    parser.add_argument("--output", type=Path, default=LAB / "data" / "project-runs")
    args = parser.parse_args()
    if args.duration <= 0 or args.max_events < 0:
        parser.error("duration must be positive and max-events nonnegative")
    if "all" in {lang.strip() for lang in args.languages.split(",")}:
        parser.error("specify Wikipedia languages explicitly, for example en,fr")

    topic = f"project-wiki-{uuid.uuid4().hex[:12]}"
    run = args.output.resolve() / topic
    run.mkdir(parents=True)
    reference_path = None
    if args.reference:
        reference_path = run / "reference.csv"
        shutil.copyfile(args.reference, reference_path)
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    spark_home = Path(os.environ.get("SPARK_HOME", "/usr/local/spark"))
    version = (spark_home / "RELEASE").read_text().split()[1]
    scala_jar = next((spark_home / "jars").glob("scala-library-*.jar"))
    scala = ".".join(scala_jar.stem.removeprefix("scala-library-").split(".")[:2])
    connector = f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"
    spark = (
        SparkSession.builder.master("local[2]").appName("ImdbWikipediaStreaming")
        .config("spark.jars.packages", connector)
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None
    producer = None
    try:
        subprocess.run(
            [sys.executable, str(LAB / "admin.py"), "--bootstrap-servers", broker, "--topic", topic],
            check=True,
        )
        reference = reference_table(spark, str(reference_path)) if reference_path else None
        query = (
            streaming_events(spark, broker, topic, reference).writeStream
            .format("parquet").option("path", str(run / "events"))
            .option("checkpointLocation", str(run / "checkpoint"))
            .trigger(processingTime="10 seconds").start()
        )
        print(f"Topic {topic}; capture log: {run / 'producer.log'}", flush=True)
        with (run / "producer.log").open("w", encoding="utf-8") as log:
            start = time.monotonic()
            start_utc = datetime.now(timezone.utc).isoformat()
            producer = subprocess.Popen(
                [
                    sys.executable, "-u", str(LAB / "wikistream_producer.py"),
                    "--bootstrap-servers", broker, "--topic", topic,
                    "--languages", args.languages, "--duration", str(args.duration),
                    "--max-events", str(args.max_events), "--output", str(run / "capture.jsonl"),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "PYTHONIOENCODING": "utf-8",
                    "WIKISTREAM_USER_AGENT": (
                        "ECE-BigData-gr04/1.0 (https://github.com/benjaminlabos/ece-big-data-processing)"
                    ),
                },
            )
            while producer.poll() is None:
                if query.awaitTermination(1):
                    raise RuntimeError("Spark query stopped before the producer finished")
            elapsed = time.monotonic() - start
        if producer.returncode:
            raise RuntimeError(f"Producer failed; see {run / 'producer.log'}")
        lines = (run / "producer.log").read_text(encoding="utf-8").splitlines()
        query.processAllAvailable()
        query.stop()
        summary = next(json.loads(line) for line in reversed(lines) if line.startswith("{"))
        evidence = collect_evidence(
            spark, run, summary, elapsed, start_utc, topic, args.duration, reference_path
        )
        if args.max_events == 1 and not evidence["smoke_event_id_verified"]:
            raise RuntimeError("The smoke event could not be traced through Spark")
    finally:
        if producer is not None and producer.poll() is None:
            producer.terminate()
            producer.wait()
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
