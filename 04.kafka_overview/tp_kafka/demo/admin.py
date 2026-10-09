"""Démonstration du cours : attendre la confirmation de création de timer.

Le fichier du professeur, inchangé, est conservé dans admin_original.py.
"""
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

config = {"bootstrap.servers": "localhost:9092"}
admin_client = AdminClient(config)
topic = "timer"

# create_topics est asynchrone : conserver le Future et attendre son résultat.
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)],
    request_timeout=15,
)
try:
    futures[topic].result(timeout=20)
    print(f"Topic créé : {topic}")
except KafkaException as exc:
    if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
        print(f"Topic déjà existant : {topic}")
    else:
        raise

metadata = admin_client.list_topics(timeout=10)
for name in metadata.topics:
    print(name)
