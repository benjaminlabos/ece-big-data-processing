"""Lab, étapes 5 et 6 : consommer le nouveau topic et écrire le texte nettoyé.

À lancer APRÈS la fin du producer. On lit les offsets présents au démarrage,
du début à la fin : pas d'arrêt arbitraire après quelques secondes de silence.
"""
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition

from config import BOOTSTRAP_SERVERS, OUTPUT_PATH, TOPIC
from text_cleaning import GutenbergCleaner


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "gutenberg-cleaning-lab",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
    })
    received = written = 0
    cleaner = GutenbergCleaner()
    partial_path = OUTPUT_PATH.with_suffix(".txt.part")
    try:
        metadata = consumer.list_topics(timeout=10)
        if TOPIC not in metadata.topics or metadata.topics[TOPIC].error is not None:
            raise RuntimeError("Topic absent ou indisponible : lancer python admin.py.")
        if len(metadata.topics[TOPIC].partitions) != 1:
            raise RuntimeError("Le lab attend exactement une partition.")
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10)
        if low != 0:
            raise RuntimeError("Des messages ont expiré : utiliser un nouveau topic pour le lab.")
        if high == low:
            raise RuntimeError("Topic vide : lancer d'abord python producer.py.")
        # Affectation explicite au début : une relance relit les mêmes messages.
        consumer.assign([TopicPartition(TOPIC, 0, low)])
        print(f"Lecture de {TOPIC}, offsets {low} à {high - 1}.")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        last_message = time.monotonic()
        with partial_path.open("w", encoding="utf-8", newline="\n") as output:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    if time.monotonic() - last_message > 30:
                        raise RuntimeError("Lecture interrompue : 30 s sans message, avant la fin.")
                    continue
                if message.error():
                    raise KafkaException(message.error())
                if message.offset() >= high:
                    break
                last_message = time.monotonic()
                raw = message.value()
                if raw is None:
                    raise RuntimeError("Message sans valeur : le topic n'est pas celui du lab.")
                line = raw.decode("utf-8")
                cleaned = cleaner.process(line)
                received += 1
                if cleaned:
                    output.write(cleaned + "\n")
                    written += 1
                    if written <= 5:
                        print(cleaned)
                if message.offset() == high - 1:
                    break
        if received != high - low:
            raise RuntimeError("Des messages manquent dans le topic.")
        if not cleaner.started or not cleaner.finished:
            raise RuntimeError("Livre incomplet : les deux balises Gutenberg sont nécessaires.")
        partial_path.replace(OUTPUT_PATH)
        print(f"{received} messages reçus ; {written} lignes nettoyées écrites.")
        print(f"Fichier : {OUTPUT_PATH}")
    finally:
        consumer.close()
        partial_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
