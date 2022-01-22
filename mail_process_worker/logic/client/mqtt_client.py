import json
import time
import os

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
        self.debug_id = "{}_{}".format(int(time.time()), os.getpid())

    @retry(times=3, delay=1)
    @timeout(10)
    def connect_server(self):
        self.client = mqtt.Client(self.client_id, self.clean_session)
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = MQTTClient.on_connect
        self.client.connect(self.broker, self.port, self.keep_alive)
        self.client.on_log = self.on_log
        return self.client

    def on_connect(client, userdata, flags, rc):
        logger.info("Result from connect: {}".format(mqtt.connack_string(rc)))
        if rc == 0:
            logger.info("Connection successful")
        else:
            logger.info("Failed to connect, return code {}\n".format(rc))
            client.reconnect()

    def on_log(self, client, userdata, level, buf):
        logger.info("[{}] mqtt log: {}".format(self.debug_id, buf))

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
            "qos": self.qos,
        }
        if uids > 1 or message.get("event") in ["MessageMove", "FlagsSet", "FlagsClear"]:
            topic = self.topic.format(user, "aggregated")
            msg_format.update({"topic": topic})
        else:
            topic = self.topic.format(user, "normal")
            msg_format.update({"topic": topic})
        self.mqtt_msgs.append(msg_format)

    @retry()
    @timeout(60)
    def publish_message(self, consumer):
        for msg in self.mqtt_msgs:
            payload = msg.get("payload", {})
            qos = msg.get("qos", 1)
            mqtt_topic = msg.get("topic")
            p = json.loads(payload)
            logger.info("SENDING MESSAGE: {} TO TOPIC: {}".format(payload, mqtt_topic))
            logger.info(f"MAIL_PROCESS LOG TRACKING: {p.get('user')}-{p.get('mailbox')}-{p.get('uids')}-{p.get('event')}")
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

    @retry(delay=2, logger=logger)
    @timeout(20)
    def publish(self, client, consumer):
        cp_msgs = self.mqtt_msgs[:]
        for msg in cp_msgs:
            payload = msg.get("payload", {})
            qos = msg.get("qos", 1)
            mqtt_topic = msg.get("topic")
            result = client.publish(mqtt_topic, payload, qos)
            _payload = json.loads(payload)
            log = f"{_payload.get('user')}-{_payload.get('mailbox')}-{_payload.get('uids')}"
            status = result[0]
            if status == 0:
                logger.info("[{}] Send message: {} to topic: {}".format(self.debug_id, payload, mqtt_topic))
                logger.info("[{}] mail process log tracking: {}".format(self.debug_id, log))
            else:
                logger.info("[{}] Failed to send `{}` to topic {}".format(self.debug_id, log, mqtt_topic))
                raise Exception
            self.commit(consumer, payload)
            self.mqtt_msgs.remove(msg)

    def commit(self, consumer, payload):
        payload = json.loads(payload)
        event_topic = payload.get("topic")
        partition = payload.get("partition")
        offset = payload.get("offset")
        KafkaConsumerClient.kafka_commit(
            consumer, event_topic, partition, offset
        )
        logger.info(
            "[{}] kafka commit - topic: {} - partition: {} - offset: {}".format(self.debug_id, event_topic, partition, offset)
        )
