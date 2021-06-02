import os
import json


CONF_FILE = os.path.join(
    os.path.dirname(os.path.abspath("config.json")), "config.json"
)

with open(CONF_FILE, "r") as f:
    config = json.load(f)


class KafkaConsumerConfig:
    consumer = config.get("KAFKA_CONSUMER")
    KAFKA_BROKER = consumer.get("KAFKA_BROKER")
    KAFKA_TOPIC = consumer.get("KAFKA_TOPIC")
    KAFKA_CONSUMER_GROUP = consumer.get("KAFKA_CONSUMER_GROUP")


class KafkaProducerConfig:
    producer = config.get("KAFKA_PRODUCER")
    KAFKA_BROKER = producer.get("KAFKA_BROKER")
    KAFKA_TOPIC = producer.get("KAFKA_TOPIC")
