from mail_process_worker.setting import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
)

import json

from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import timeout, retry

from kafka.structs import OffsetAndMetadata
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


@retry(times=3, delay=1)
@timeout(10)
def get_consumer():
    logger.info("connect kafka")
    consumer = KafkaConsumer(
        *KafkaConsumerConfig.KAFKA_TOPIC,
        group_id=KafkaConsumerConfig.KAFKA_CONSUMER_GROUP,
        bootstrap_servers=KafkaConsumerConfig.KAFKA_BROKER,
        auto_offset_reset=KafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda x: json.loads(x.decode("utf-8", "ignore")),
        enable_auto_commit=KafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT,
        max_poll_records=KafkaConsumerConfig.KAFKA_MAX_POLL_RECORDS,
    )
    logger.info("connect success")
    return consumer


@retry(times=3, delay=1, logger=logger)
@timeout(10)
def get_producer():
    producer = KafkaProducer(
        bootstrap_servers=KafkaProducerConfig.KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=2,
    )
    return producer


@timeout(30)
def send_to_kafka(consumer: KafkaConsumer, user_event: dict):
    producer = get_producer()
    for user in user_event:
        events = user_event[user]
        events.sort(key=lambda x: x[0])
        for event in events:
            partition = event[1].get("partition")
            tp = KafkaProducerConfig.KAFKA_TOPIC.format(partition)
            logger.info(
                f"SENDING TO TOPIC: {tp}| USER {user} ==> EVENT {event[1]}"
            )
            producer.send(
                tp,
                key=bytes(user, "utf-8"),
                value=event[1],
            )
            producer.flush()
            topic = event[1].get("topic")
            offset = event[1].get("offset")
            kafka_commit(consumer, topic, partition, offset)
            logger.info(f"Done | USER {user}")


def kafka_commit(consumer, topic, partition, offset):
    tp = TopicPartition(topic, partition)
    consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
    logger.info(
        f"KAFKA COMMIT - TOPIC: {topic} - PARTITION: {partition} - OFFSET: {offset}"
    )
