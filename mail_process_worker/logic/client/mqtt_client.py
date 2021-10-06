import json
import time

import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqtt_publish

from mail_process_worker.setting import MQTTConfig
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import retry, timeout
from mail_process_worker.logic.client.kafka_client import KafkaConsumerClient


class MQTTClient:
    def __init__(self) -> None:
        self.client = None
        self.client_id = MQTTConfig.CLIENT_ID
        self.broker = MQTTConfig.MQTT_BROKER
        self.port = MQTTConfig.MQTT_PORT
        self.username = MQTTConfig.MQTT_USERNAME
        self.password = MQTTConfig.MQTT_PASSWORD
        self.topic = MQTTConfig.MQTT_TOPIC
        self.qos = MQTTConfig.MQTT_QoS
        self.keep_alive = MQTTConfig.MQTT_KEEPALIVE
        self.clean_session = MQTTConfig.MQTT_CLEAN_SESSION
        self.mqtt_msgs = []

    @retry(times=3, delay=1)
    @timeout(10)
    def connect_server(self):
        self.client = mqtt.Client(self.client_id, self.clean_session)
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = MQTTClient.on_connect
        self.client.connect(self.broker, self.port, self.keep_alive)
        return self.client

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        logger.info("Result from connect: {}".format(mqtt.connack_string(rc)))
        if rc == 0:
            logger.info("Connection successful")
        else:
            logger.info("Failed to connect, return code {}\n".format(rc))
            client.reconnect()

    @staticmethod
    def on_log(client, userdata, level, buf):
        logger.info(buf)

    def ordered_message(self, user_messages: dict):
        for user in user_messages:
            messages = user_messages[user]
            messages.sort(key=lambda x: x[0])
            for priority, message in messages:
                self.create_mqtt_message(message)

    def create_mqtt_message(self, message: dict):
        uids = len(message.get("uids", []))
        user = message.get("user")
        special_char = user.find("@")
        username = user[:special_char]
        domain = user[special_char + 1 :]
        payload = json.dumps(message)
        msg_format = {
            "payload": payload,
            "qos": self.qos
        }
        if uids > 1 or message.get("event") == "MessageMove":
            topic = f"bulk/{self.topic.format(domain, username)}"
            msg_format.update({"topic": topic})
        else:
            topic = self.topic.format(domain, username)
            msg_format.update({"topic": topic})
        self.mqtt_msgs.append(msg_format)

    @retry()
    @timeout(60)
    def publish_message(self, consumer):
        for msg in self.mqtt_msgs:
            payload = msg.get("payload", {})
            qos = msg.get("qos", 1)
            mqtt_topic = msg.get("topic")
            logger.info("SENDING MESSAGE: {} TO TOPIC: {}".format(payload, mqtt_topic))
            mqtt_publish.single(
                topic=mqtt_topic,
                payload=payload,
                qos=qos,
                hostname=self.broker,
                port=self.port,
                client_id=self.client_id,
                auth={"username": self.username, "password": self.password},
            )
            self.commit(consumer, payload)
        self.mqtt_msgs.clear()

    def publish_multiple_message(self):
        mqtt_publish.multiple(
            msgs=self.mqtt_msgs,
            hostname=self.broker,
            port=self.port,
            client_id=self.client_id,
            auth={"username": self.username, "password": self.password},
        )
        self.mqtt_msgs.clear()

    def commit(self, consumer, payload):
        payload = json.loads(payload)
        event_topic = payload.get("topic")
        partition = payload.get("partition")
        offset = payload.get("offset")
        KafkaConsumerClient.kafka_commit(
            consumer, event_topic, partition, offset
        )
