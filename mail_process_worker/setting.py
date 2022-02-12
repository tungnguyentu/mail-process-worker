from environs import Env
import os

env = Env()
env.read_env()


class KafkaClientConfig:
    KAFKA_BROKER = env.list("KAFKA_BROKER")
    KAFKA_CONSUMER_TOPIC = env.list("KAFKA_CONSUMER_TOPIC")
    KAFKA_CONSUMER_GROUP = env.str("KAFKA_CONSUMER_GROUP")
    KAFKA_ENABLE_AUTO_COMMIT = env.bool("KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_AUTO_OFFSET_RESET = env.str("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = env.int("KAFKA_MAX_POLL_RECORDS")
    KAFKA_POLL_TIMEOUT = env.int("KAFKA_POLL_TIMEOUT")

    KAFKA_PRODUCER_NORMAL_TOPIC = env.str("KAFKA_PRODUCER_NORMAL_TOPIC")
    KAFKA_PRODUCER_AGGREGATED_TOPIC = env.str("KAFKA_PRODUCER_AGGREGATED_TOPIC")


class MQTTConfig:
    CLIENT_ID = os.environ.get("CLIENT_ID")
    MQTT_BROKER = env.str("MQTT_BROKER")
    MQTT_PORT = env.int("MQTT_PORT")
    MQTT_USERNAME = env.str("MQTT_USERNAME")
    MQTT_PASSWORD = env.str("MQTT_PASSWORD")
    MQTT_TOPIC = env.str("MQTT_TOPIC")
    MQTT_QoS = env.int("MQTT_QoS")
    MQTT_KEEPALIVE = env.int("MQTT_KEEPALIVE")
    MQTT_CLEAN_SESSION = env.str("MQTT_CLEAN_SESSION")


class RedisConfig:
    REDIS_URL = env.str("REDIS_URL")


class WorkerConfig:
    WINDOW_DURATION = env.int("WINDOW_DURATION")
    NUMBER_OF_MESSAGE = env.int("NUMBER_OF_MESSAGE")


class SentryConfig:
    SENTRY_DSN = env.str("SENTRY_DSN")
