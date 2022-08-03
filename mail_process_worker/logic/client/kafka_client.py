import json

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata

from mail_process_worker.setting import KafkaClientConfig
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import retry, timeout


class KafkaConsumerClient:
    def __init__(self) -> None:
        self.consumer = None
        self.topics = KafkaClientConfig.KAFKA_CONSUMER_TOPIC
        self.group_id = KafkaClientConfig.KAFKA_CONSUMER_GROUP
        self.bootstrap_servers = KafkaClientConfig.KAFKA_BROKER
        self.auto_offset_reset = KafkaClientConfig.KAFKA_AUTO_OFFSET_RESET
        self.value_deserializer = lambda x: json.loads(
            x.decode("utf-8", "ignore")
        )
        self.enable_auto_commit = KafkaClientConfig.KAFKA_ENABLE_AUTO_COMMIT
        self.max_poll_records = KafkaClientConfig.KAFKA_MAX_POLL_RECORDS
        self.poll_timeout = KafkaClientConfig.KAFKA_POLL_TIMEOUT

    @retry(times=3, delay=1)
    @timeout(10)
    def create_consumer(self):
        logger.info(self.bootstrap_servers)
        self.consumer = KafkaConsumer(
            *self.topics,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=self.value_deserializer,
            enable_auto_commit=self.enable_auto_commit,
            max_poll_records=self.max_poll_records,
        )

    def poll_message(self):
        msg = self.consumer.poll(self.poll_timeout)
        return msg

    @staticmethod
    def kafka_commit(consumer, topic, partition, offset):
        tp = TopicPartition(topic, partition)
        consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
        logger.info(
            f"KAFKA COMMIT - TOPIC: {topic} - PARTITION: {partition} - OFFSET: {offset}"
        )


class KafkaProducerClient:
    def __init__(self) -> None:
        self.bootstrap_servers = KafkaClientConfig.KAFKA_BROKER
        self.normal_topic = KafkaClientConfig.KAFKA_PRODUCER_NORMAL_TOPIC
        self.aggregated_topic = (
            KafkaClientConfig.KAFKA_PRODUCER_AGGREGATED_TOPIC
        )
        self.value_serializer = lambda x: json.dumps(x).encode("utf-8")
        self.kafka_msgs = []

    def ordered_message(self, user_messages: dict):
        for user in user_messages:
            messages = user_messages[user]
            messages.sort(key=lambda x: x[0])
            for priority, message in messages:
                self.create_kafka_message(message)

    def create_kafka_message(self, message: dict):
        user = message.get("user")
        _, _, domain = user.partition("@")
        msg_format = {"payload": message}
        if  message.get("event") in KafkaClientConfig.KAFKA_EVENT_AGGREGATE:
            if domain in KafkaClientConfig.KAFKA_IGNORE_DOMAIN:
                return
            topic = self.aggregated_topic
            msg_format.update({"key": user, "topic": topic})
        else:
            topic = self.normal_topic
            msg_format.update({"key": user, "topic": topic})
        self.kafka_msgs.append(msg_format)

    @retry(times=3, delay=1, logger=logger)
    @timeout(60)
    def send_message(self, consumer: KafkaConsumer):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.value_serializer,
            acks="all"
        )
        for msg in self.kafka_msgs:
            payload = msg.get("payload", {})
            kafka_topic = msg.get("topic")
            kafka_key = msg.get("key")
            uids = payload.get("uids")
            if uids:
                self.split_event(payload, uids, kafka_topic, kafka_key, producer)
            else:
                uids = [payload.get("uid")]
                payload.pop("uid")
                payload["uids"] = uids
                logger.info(
                    "Sending message: {} to topic: {}".format(payload, kafka_topic)
                )
                producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()
            self.commit(consumer, payload)
        self.kafka_msgs.clear()
    
    def split_event(self, payload: dict, uids: list, kafka_topic: str, kafka_key: str, producer: KafkaProducer):
        slice = KafkaClientConfig.KAFKA_SLICE_SIZE
        logger.info(
            "Sending message: {} to topic: {}".format(payload, kafka_topic)
        )
        while len(uids) >= slice:
            p = uids[:slice]
            uids = uids[slice:]
            payload["uids"] = p
            producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
            producer.flush()
        else:
            if uids:
                payload["uids"] = uids                
                producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()

    def commit(self, consumer, payload):
        event_topic = payload.get("topic")
        partition = payload.get("partition")
        offset = payload.get("offset")
        tp = TopicPartition(event_topic, partition)
        consumer.commit({tp: OffsetAndMetadata(offset + 1, None)})
        logger.info(
            f"Kafka commit - topic: {event_topic} - partition: {partition} - offset: {offset}"
        )
