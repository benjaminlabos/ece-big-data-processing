"""Verify Kafka offsets and distinct matching events across a controlled Spark restart."""

import csv
import json
import os
import subprocess
import sys
import time
import uuid

from confluent_kafka import Producer

from project_stream import (
    LAB, measure_matches, reference_table, resume_query, spark_session, start_query, streaming_events,
)


TITLE = "Film d'été, le retour"


def publish(producer, topic, event_ids):
    for event_id in event_ids:
        payload = {
            "title": TITLE,
            "wiki": "frwiki",
            "server_name": "fr.wikipedia.org",
            "namespace": 0,
            "type": "edit",
            "timestamp": int(time.time()) - 2,
            "revision": {"new": 123},
            "meta": {"id": event_id, "domain": "fr.wikipedia.org"},
        }
        producer.produce(topic, key=event_id, value=json.dumps(payload).encode("utf-8"))
    if producer.flush(15):
        raise RuntimeError("Synthetic Kafka records did not finish delivery")


def main():
    topic = f"project-wiki-checkpoint-{uuid.uuid4().hex[:12]}"
    run = LAB / "data" / "project-runs" / topic
    run.mkdir(parents=True)
    reference_path = run / "reference.csv"
    with reference_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["wiki", "title", "imdb_id", "wikidata_id"])
        writer.writerow(["frwiki", TITLE, "tt0000001", "Q1"])
        writer.writerow(["frwiki", "__Film__d'été,_le__retour__", "tt0000001", "Q1"])
        writer.writerow(["frwiki", TITLE, "tt0000002", "Q1"])
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    subprocess.run(
        [sys.executable, str(LAB / "admin.py"), "--bootstrap-servers", broker, "--topic", topic],
        check=True,
    )
    producer = Producer({"bootstrap.servers": broker, "enable.idempotence": True})
    spark = spark_session("ImdbWikipediaCheckpointCheck")
    query = None
    try:
        reference = reference_table(spark, str(reference_path))
        events = streaming_events(spark, broker, topic, reference)
        query = start_query(events, run)
        started = time.monotonic()
        publish(producer, topic, ["synthetic-a"])
        query.processAllAvailable()
        progress = query.lastProgress
        if not progress or progress["numInputRows"] != 1:
            raise AssertionError("Expected one Kafka record before restart")
        query, restart = resume_query(query, events, run, progress, time.monotonic() - started)
        publish(producer, topic, ["synthetic-a", "synthetic-b"])
        query.processAllAvailable()
        progress = query.lastProgress
        if not progress or progress["batchId"] <= restart["previous_batch_id"]:
            raise AssertionError("Restart did not advance the Spark checkpoint")
        restart["last_batch_id"] = progress["batchId"]
        restart["last_end_offset"] = progress["sources"][0]["endOffset"]
        query.stop()
        rows = spark.read.parquet(str(run / "events"))
        sequence = [
            [row.partition, row.offset, row.event_id]
            for row in rows.orderBy("partition", "offset").select("partition", "offset", "event_id").collect()
        ]
        assert sequence == [[0, 0, "synthetic-a"], [0, 1, "synthetic-a"], [0, 2, "synthetic-b"]]
        counts, latency, samples = measure_matches(rows)
        assert counts["kafka_records"] == 3
        assert counts["duplicate_kafka_offsets"] == 0
        assert counts["duplicate_event_records"] == 1
        assert counts["matched_events"] == 2
        assert counts["matched_pages"] == 1
        assert [row["offset"] for row in samples] == [0, 2]
        assert all(len(row["imdb_matches"]) == 2 for row in samples)
        assert latency["event_to_spark_ms"]["count"] == 2
        for row in samples:
            assert row["event_to_spark_ms"] == row["event_to_kafka_ms"] + row["kafka_to_spark_ms"]
        summary = {
            "scope": "synthetic_kafka_checkpoint_and_duplicate_check",
            "spark_version": spark.version,
            "topic": topic,
            "restart": restart,
            "kafka_sequence": sequence,
            **counts,
            "examples": samples,
        }
        report = json.dumps(summary, ensure_ascii=False, indent=2)
        (run / "checkpoint-check.json").write_text(report + "\n", encoding="utf-8")
        print(report, flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
