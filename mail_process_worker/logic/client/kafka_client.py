import json
import ssl

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from mail_process_worker.setting import KafkaConsumerConfig, KafkaAuth
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import retry, timeout

context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

class KafkaConsumerClient:
    def __init__(self) -> None:
        self.consumer = None
        self.topics = KafkaConsumerConfig.KAFKA_CONSUMER_TOPIC
        self.group_id = KafkaConsumerConfig.KAFKA_CONSUMER_GROUP
        self.bootstrap_servers = KafkaConsumerConfig.KAFKA_BROKER
        self.sasl_plain_username = KafkaAuth.SASL_PLAIN_USERNAME
        self.sasl_plain_password = KafkaAuth.SASL_PLAIN_PASSWORD
        self.security_protocol = KafkaAuth.SECURITY_PROTOCOL
        self.ssl_context = context
        self.sasl_mechanism = KafkaAuth.SASL_MECHANISM
        self.auto_offset_reset = KafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET
        self.value_deserializer = lambda x: json.loads(
            x.decode("utf-8", "ignore")
        )
        self.enable_auto_commit = KafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT
        self.max_poll_records = KafkaConsumerConfig.KAFKA_MAX_POLL_RECORDS
        self.poll_timeout = KafkaConsumerConfig.KAFKA_POLL_TIMEOUT

    @retry(times=3, delay=1)
    @timeout(10)
    def create_consumer(self):
        logger.info(self.bootstrap_servers)
        self.consumer = KafkaConsumer(
            *self.topics,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            sasl_mechanism=self.sasl_mechanism,
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
