"""Create and verify the one-partition topic used by the lab."""
import argparse
import os
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--topic", default=os.getenv("WIKISTREAM_TOPIC", "wikistreams"))
    args = parser.parse_args()
    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    # A container being started does not mean that its broker is ready yet.
    for attempt in range(30):
        try:
            admin.list_topics(timeout=2)
            break
        except KafkaException:
            if attempt == 29:
                raise
            time.sleep(1)
    future = admin.create_topics([NewTopic(args.topic, num_partitions=1, replication_factor=1)])[args.topic]
    try:
        future.result(timeout=30)
        print(f"Topic created: {args.topic}")
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        print(f"Topic already exists: {args.topic}")
    topic = admin.list_topics(topic=args.topic, timeout=10).topics[args.topic]
    if topic.error:
        raise KafkaException(topic.error)
    if len(topic.partitions) != 1:
        raise RuntimeError("The lab requires exactly one partition.")
    print(f"Verified: {args.topic}, partitions=1, broker={args.bootstrap_servers}")


if __name__ == "__main__":
    main()
