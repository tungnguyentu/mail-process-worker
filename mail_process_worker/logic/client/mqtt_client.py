import json

import paho.mqtt.client as mqtt

from mail_process_worker.setting import MQTTConfig
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import retry, timeout


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

    @retry(times=3, delay=1)
    @timeout(10)
    def connect_server(self):
        self.client = mqtt.Client(self.client_id)
        self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = MQTTClient.on_connect
        self.client.connect(self.broker, self.port)
        return self.client

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        logger.info(f"Result from connect: {mqtt.connack_string(rc)}")

    def publish_message(self, message):
        logger.info(f"MESSAGE: {message}")
        return
        self.client.publish(self.topic, message, qos=self.qos)
