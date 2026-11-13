"""Send filtered Wikimedia recent changes to Kafka for a bounded duration."""
import argparse
import json
import os
from pathlib import Path
import socket
import time

from confluent_kafka import Producer
import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def matches(event, languages, pages, exclude_bots):
    """Filter at the producer, before sending any event to Kafka."""
    return (
        event.get("type") == "edit"
        and event.get("meta", {}).get("domain") != "canary"
        and ("all" in languages or event.get("server_name") in {f"{lang}.wikipedia.org" for lang in languages})
        and (not pages or event.get("title") in pages)
        and (not exclude_bots or not event.get("bot", False))
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--topic", default=os.getenv("WIKISTREAM_TOPIC", "wikistreams"))
    parser.add_argument("--languages", default="fr", help="Comma-separated Wikipedia languages, or all")
    parser.add_argument("--page", action="append", default=[], help="Exact page title; repeat for several pages")
    parser.add_argument("--exclude-bots", action="store_true")
    parser.add_argument("--duration", type=float, default=600, help="Maximum capture duration in seconds")
    parser.add_argument("--max-events", type=int, default=0, help="Stop after this many messages; 0 means no limit")
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "data" / "wiki_events.jsonl"), help="Local audit copy (JSON Lines)")
    args = parser.parse_args()
    if args.duration <= 0 or args.max_events < 0:
        parser.error("duration must be positive and max-events nonnegative")
    languages = {item.strip() for item in args.languages.split(",") if item.strip()}
    if not languages:
        parser.error("At least one language is required")
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": socket.gethostname(),
        "compression.type": "lz4",
        "enable.idempotence": True,
        "delivery.timeout.ms": 30000,
    })
    counts = {"seen": 0, "sent": 0, "delivered": 0, "duplicates": 0, "errors": 0}

    def on_delivery(error, message):
        if error:
            counts["errors"] += 1
            print(f"Kafka delivery failed: {error}", flush=True)
        else:
            counts["delivered"] += 1

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    # The SSE connection can resume with an event already received.
    seen_ids = set()
    if output.exists():
        for line in output.read_text(encoding="utf-8").splitlines():
            previous_id = json.loads(line).get("meta", {}).get("id")
            if previous_id:
                seen_ids.add(previous_id)
    deadline = time.monotonic() + args.duration
    last_event_id = None
    print(f"Following languages={sorted(languages)}, pages={args.page or 'all'}, exclude_bots={args.exclude_bots}", flush=True)
    try:
        with output.open("a", encoding="utf-8") as audit:
            while time.monotonic() < deadline and (not args.max_events or counts["sent"] < args.max_events):
                headers = {
                    "User-Agent": os.getenv(
                        "WIKISTREAM_USER_AGENT", "ECE-StructuredStreaming-Lab/1.0 (educational project)"
                    ),
                    "Accept": "text/event-stream",
                }
                if last_event_id:
                    headers["Last-Event-ID"] = last_event_id
                try:
                    # No historical 'since' date: consume current events.
                    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 10)) as response:
                        response.raise_for_status()
                        response.encoding = "utf-8"
                        data_lines = []
                        pending_event_id = None
                        for line in response.iter_lines(decode_unicode=True):
                            producer.poll(0)
                            if time.monotonic() >= deadline:
                                break
                            if line.startswith("id:"):
                                pending_event_id = line[3:].strip()
                            elif line.startswith("data:"):
                                data_lines.append(line[5:].lstrip())
                            elif not line and data_lines:
                                if pending_event_id is not None:
                                    last_event_id = pending_event_id
                                    pending_event_id = None
                                try:
                                    event = json.loads("\n".join(data_lines))
                                except json.JSONDecodeError:
                                    data_lines = []
                                    continue
                                data_lines = []
                                counts["seen"] += 1
                                if not matches(event, languages, args.page, args.exclude_bots):
                                    continue
                                event_id = event.get("meta", {}).get("id")
                                if event_id and event_id in seen_ids:
                                    counts["duplicates"] += 1
                                    continue
                                value = json.dumps(event, ensure_ascii=False)
                                while True:
                                    try:
                                        producer.produce(args.topic, key=event.get("meta", {}).get("id", ""), value=value.encode("utf-8"), on_delivery=on_delivery)
                                        break
                                    except BufferError:
                                        producer.poll(1)
                                audit.write(value + "\n")
                                if event_id:
                                    seen_ids.add(event_id)
                                counts["sent"] += 1
                                if counts["sent"] == 1 or counts["sent"] % 25 == 0:
                                    print(f"sent={counts['sent']} | {event['server_name']} | {event['title']}", flush=True)
                                if args.max_events and counts["sent"] >= args.max_events:
                                    break
                except requests.RequestException as exc:
                    print(f"Wikimedia connection interrupted: {exc}; retrying while time remains", flush=True)
                    time.sleep(min(2, max(0, deadline - time.monotonic())))
    except KeyboardInterrupt:
        print("Capture interrupted; flushing queued messages", flush=True)
    finally:
        # confluent_kafka.Producer has flush(), not close().
        pending = producer.flush(30)
        summary = {**counts, "pending": pending, "topic": args.topic, "languages": sorted(languages)}
        print(json.dumps(summary), flush=True)
    if pending or counts["errors"] or not counts["delivered"]:
        raise SystemExit("No complete delivery confirmed; check the network, filters and Kafka.")


if __name__ == "__main__":
    main()
