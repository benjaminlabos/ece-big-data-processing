"""Lab, étape 2 : créer un nouveau topic avec une partition."""
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "gutenberg-book-lines"


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics(
        [NewTopic(TOPIC, num_partitions=1, replication_factor=1)],
        request_timeout=15,
    )
    try:
        # La création est asynchrone : attendre sa confirmation.
        futures[TOPIC].result(timeout=20)
        print(f"Topic créé : {TOPIC}")
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"Topic déjà existant : {TOPIC}")
        else:
            raise
    metadata = admin.list_topics(timeout=10)
    topic = metadata.topics[TOPIC]
    if topic.error is not None:
        raise KafkaException(topic.error)
    if len(topic.partitions) != 1:
        raise RuntimeError("Le lab attend exactement une partition.")
    print("Partitions : 1 ; réplication : 1")


if __name__ == "__main__":
    try:
        main()
    except (KafkaException, TimeoutError, RuntimeError) as exc:
        raise SystemExit(f"Création impossible : {exc}. Vérifier Docker/Kafka.")
