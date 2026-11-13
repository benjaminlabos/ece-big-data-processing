"""Run a deterministic event-time and watermark exercise with local JSON files."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession, functions as F


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("WikimediaLateEventDemo")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        with TemporaryDirectory(prefix="wiki-late-events-") as directory:
            root = Path(directory)
            source = root / "input"
            source.mkdir()
            events = (
                spark.readStream.schema("event_at STRING")
                .option("maxFilesPerTrigger", 1)
                .json(str(source))
                .withColumn("event_time", F.to_timestamp("event_at"))
                .withColumn("processed_at", F.current_timestamp())
            )
            counts = (
                events.withWatermark("event_time", "2 minutes")
                .groupBy(F.window("event_time", "1 minute"))
                .agg(
                    F.count("*").alias("events"),
                    F.max("processed_at").alias("last_processed_at"),
                )
            )
            query = (
                counts.writeStream.format("memory")
                .queryName("wiki_late_demo")
                .outputMode("update")
                .option("checkpointLocation", str(root / "checkpoint"))
                .start()
            )
            try:
                stages = [
                    ("initial", ["12:00:20"]),
                    ("advance_watermark", ["12:05:00"]),
                    ("late_inputs", ["12:04:30", "12:00:30"]),
                ]
                for stage, times in stages:
                    records = [{"event_at": f"2026-01-01T{time}Z"} for time in times]
                    pending = root / f"{stage}.json"
                    pending.write_text(
                        "".join(json.dumps(record) + "\n" for record in records),
                        encoding="utf-8",
                    )
                    # Publish only complete files and wait for each stage before the next.
                    pending.replace(source / pending.name)
                    query.processAllAvailable()
                    progress = query.lastProgress
                    print(json.dumps({
                        "stage": stage,
                        "input": records,
                        "last_batch": progress["batchId"],
                        "watermark": progress["eventTime"]["watermark"],
                    }), flush=True)

                dropped = sum(
                    operator["numRowsDroppedByWatermark"]
                    for batch in query.recentProgress
                    for operator in batch["stateOperators"]
                )
                rows = spark.table("wiki_late_demo").select(
                    F.date_format("window.start", "HH:mm:ss").alias("window_start_utc"),
                    "events",
                    F.date_format("last_processed_at", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .alias("last_processed_at_utc"),
                ).orderBy("window_start_utc").collect()
                actual = [(row.window_start_utc, row.events) for row in rows]
                expected = [("12:00:00", 1), ("12:04:00", 1), ("12:05:00", 1)]
                if actual != expected or dropped != 1:
                    raise RuntimeError(f"Expected {expected} and one late drop; got {actual}, {dropped}")
                print(json.dumps({
                    "numRowsDroppedByWatermark": dropped,
                    "windows": [row.asDict() for row in rows],
                }, default=str), flush=True)
            finally:
                query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
