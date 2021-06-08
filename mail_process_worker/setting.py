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
    KAFKA_ENABLE_AUTO_COMMIT = consumer.get("KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_AUTO_OFFSET_RESET = consumer.get("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = consumer.get("KAFKA_MAX_POLL_RECORDS")


class KafkaProducerConfig:
    producer = config.get("KAFKA_PRODUCER")
    KAFKA_BROKER = producer.get("KAFKA_BROKER")
    KAFKA_TOPIC = producer.get("KAFKA_TOPIC")


class WorkerConfig:
    worker = config.get("WORKER")
    WINDOW_DURATION = worker.get("WINDOW_DURATION")
    NUMBER_OF_MESSAGE = worker.get("NUMBER_OF_MESSAGE")
