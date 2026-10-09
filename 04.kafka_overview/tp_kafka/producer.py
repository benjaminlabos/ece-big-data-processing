"""Lab, étape 4 : lire le livre ligne par ligne et envoyer chaque ligne."""
import socket

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from config import BOOK_PATH, BOOTSTRAP_SERVERS, TOPIC


def main():
    if not BOOK_PATH.is_file():
        raise FileNotFoundError("Lancer d'abord : python download_book.py")
    metadata = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=10)
    if TOPIC not in metadata.topics or metadata.topics[TOPIC].error is not None:
        raise RuntimeError("Le topic est absent ou indisponible. Lancer python admin.py.")
    if len(metadata.topics[TOPIC].partitions) != 1:
        raise RuntimeError("Le lab attend exactement une partition.")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": socket.gethostname(),
        "enable.idempotence": True,
        "message.timeout.ms": 30000,
        "allow.auto.create.topics": False,
    })
    failures = []
    delivered = 0

    def delivery_report(error, message):
        nonlocal delivered
        if error is not None:
            failures.append(str(error))
        else:
            delivered += 1

    sent = 0
    try:
        with BOOK_PATH.open(encoding="utf-8-sig") as book:
            for number, line in enumerate(book, start=1):
                # Les lignes vides sont aussi envoyées. Pas de nettoyage ici.
                value = line.rstrip("\r\n").encode("utf-8")
                while True:
                    try:
                        producer.produce(
                            topic=TOPIC, partition=0,
                            key=str(number).encode("utf-8"), value=value,
                            on_delivery=delivery_report,
                        )
                        break
                    except BufferError:
                        # Laisser Kafka vider la file locale avant de réessayer.
                        producer.poll(0.1)
                        if failures:
                            raise RuntimeError(failures[0])
                producer.poll(0)
                sent += 1
                if failures:
                    raise RuntimeError(failures[0])
        remaining = producer.flush(timeout=40)
        if remaining or failures or delivered != sent:
            raise RuntimeError(
                f"Envoi incomplet : {delivered}/{sent} confirmés, "
                f"{remaining} en attente ; erreurs : {failures[:3]}"
            )
        print(f"{delivered} lignes envoyées et confirmées dans {TOPIC}.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

