"""Replay an existing Wikimedia audit through a fresh Kafka topic and Spark."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

from confluent_kafka import Producer

from project_stream import LAB, measure_matches, reference_table, spark_session, streaming_events


def publish_capture(broker, topic, capture):
    producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True})
    ids, errors = [], []

    def delivery(error, message):
        if error:
            errors.append(str(error))

    with capture.open(encoding="utf-8") as handle:
        for line in handle:
            event_id = json.loads(line).get("meta", {}).get("id")
            while True:
                try:
                    producer.produce(topic, key=event_id, value=line.encode("utf-8"), on_delivery=delivery)
                    break
                except BufferError:
                    producer.poll(1)
            ids.append(event_id)
            producer.poll(0)
    if producer.flush(30) or errors or not ids:
        raise RuntimeError(f"Replay publication failed: {errors[:3]}")
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=LAB / "data" / "project-runs")
    args = parser.parse_args()
    topic = f"project-wiki-replay-{uuid.uuid4().hex[:12]}"
    run = args.output.resolve() / topic
    run.mkdir(parents=True)
    capture, reference_path = run / "capture.jsonl", run / "reference.csv"
    shutil.copyfile(args.capture, capture)
    shutil.copyfile(args.reference, reference_path)
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    spark = spark_session("ImdbWikipediaReplay")
    query = None
    try:
        subprocess.run(
            [sys.executable, str(LAB / "admin.py"), "--bootstrap-servers", broker, "--topic", topic],
            check=True,
        )
        reference = reference_table(spark, str(reference_path))
        ids = publish_capture(broker, topic, capture)
        query = (
            streaming_events(spark, broker, topic, reference).writeStream.format("parquet")
            .option("path", str(run / "events")).option("checkpointLocation", str(run / "checkpoint"))
            .trigger(availableNow=True).start()
        )
        if not query.awaitTermination(300):
            raise TimeoutError("Spark replay did not reach the end of the Kafka topic")
        rows = spark.read.parquet(str(run / "events")).persist()
        ordered = rows.orderBy("partition", "offset").select("partition", "offset", "event_id")
        expected = [(0, offset, event_id) for offset, event_id in enumerate(ids)]
        if [tuple(row) for row in ordered.toLocalIterator()] != expected:
            raise AssertionError("Kafka offsets or Spark event IDs differ from the source capture")
        counts, _, samples = measure_matches(rows)
        summary = {
            "scope": "replay_of_existing_wikimedia_capture",
            "spark_version": spark.version,
            "topic": topic,
            "capture_sha256": hashlib.sha256(capture.read_bytes()).hexdigest(),
            "reference_sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
            "audit_event_ids_and_offsets_verified": True,
            **counts,
            "examples": [
                {key: value for key, value in sample.items() if not key.endswith("_ms")}
                for sample in samples
            ],
        }
        report = json.dumps(summary, ensure_ascii=False, indent=2)
        (run / "summary.json").write_text(report + "\n", encoding="utf-8")
        print(report, flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
