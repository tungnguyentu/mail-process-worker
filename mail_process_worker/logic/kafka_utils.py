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
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
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
    )
    return producer


@timeout(30)
def send_to_kafka(consumer: KafkaConsumer, user_event: dict):
    producer = get_producer()
    for user in user_event:
        events = user_event[user]
        events.sort(key=lambda x: x[0])
        for event in events:
            logger.info(f"Send event to kafka| {user=} ==> {event[1]=}")
            producer.send(
                KafkaProducerConfig.KAFKA_TOPIC,
                key=bytes(user, "utf-8"),
                value=event[1],
            )
            producer.flush()
            topic = event[1].get("topic")
            partition = event[1].get("partition")
            offset = event[1].get("offset")
            kafka_commit(consumer, topic, partition, offset)
            logger.info(f"Done | {user=}")


def kafka_commit(consumer, topic, partition, offset):
    tp = TopicPartition(topic, partition)
    consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
    logger.info(
        f"KAFKA COMMIT - TOPIC: {topic} - PARTITION: {partition} - OFFSET: {offset}"
    )


def get_topic_partition(data):
    topic = data.get("topic", None)
    partition = int(data.get("partition"))
    tp = TopicPartition(topic, partition)
    return tp


def get_offset_and_timestamp(tp, consumer, timestamp_start, timestamp_end):
    offset_and_timestamp_start = consumer.offsets_for_times(
        {tp: int(timestamp_start)}
    )
    offset_and_timestamp_end = consumer.offsets_for_times(
        {tp: int(timestamp_end)}
    )

    offset_and_timestamp_start = list(offset_and_timestamp_start.values())[0]
    offset_and_timestamp_end = list(offset_and_timestamp_end.values())[0]
    if offset_and_timestamp_start is None or offset_and_timestamp_end is None:
        logger.info(f"offset could not found")
        return None
    return offset_and_timestamp_start, offset_and_timestamp_end


def get_offsets(data, consumer):
    logger.info(data)

    offset_start = data.get("offset_start", None)
    offset_end = data.get("offset_end", None)

    if offset_start is None:
        tp = get_topic_partition(data)
        timestamp_start = data.get("timestamp_start", None)
        timestamp_end = data.get("timestamp_end", None)

        (
            offset_timestamp_start,
            offset_timestamp_end,
        ) = get_offset_and_timestamp(
            tp, consumer, timestamp_start, timestamp_end
        )

        offset_start = offset_timestamp_start[0]
        offset_end = offset_timestamp_end[0]

    return offset_start, offset_end
