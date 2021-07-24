import time
import calendar

from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import timeout
from mail_process_worker.logic.client.mqtt_client import MQTTClient
from mail_process_worker.logic.client.kafka_client import KafkaConsumerClient
from mail_process_worker.setting import WorkerConfig


class HandleEvent:
    def __init__(self) -> None:
        self.user_events = {}
        self.new_event = {}
        self.messages = []
        self.mqtt = MQTTClient()
        self.consumer = KafkaConsumerClient()
        self.consumer.create_consumer()

    def get_current_timestamp(self):
        timestamp = calendar.timegm(time.gmtime())
        return timestamp

    def set_priority(self, data: dict):
        if len(self.messages) == WorkerConfig.NUMBER_OF_MESSAGE:
            self.mqtt.ordered_message(self.user_events)
            self.mqtt.publish_message(self.consumer.consumer)
            self.user_events.clear()
            self.new_event.clear()
            self.messages.clear()

        self.messages.append(data)
        logger.info(f"set priority for {data['event']}")
        event_priority = {
            "MailboxCreate": 1,
            "MailboxRename": 2,
            "MessageNew": 3,
            "MessageAppend": 4,
            "FlagsSet": 5,
            "FlagsClear": 5,
            "MessageMove": 6,
            "MessageTrash": 7,
            "MailboxDelete": 8,
        }
        event_name = data["event"]
        user = data.get("user", None)
        if not user:
            return
        exist_user = self.user_events.get(user, None)
        if not exist_user:
            self.user_events.update({user: []})
        self.user_events[user].append((event_priority[event_name], data))
        logger.info(f"set priority for {data['event']} | DONE")

    @timeout(10)
    def custom_event(self, event_name: str, data: dict):
        if event_name == "MessageMove":
            user = data["user"]
            if data["event"] == "MessageAppend":
                exist_user = self.new_event.get(user, None)
                if not exist_user:
                    self.new_event.update(
                        {
                            user: {
                                "new_uids": [],
                            }
                        }
                    )
                self.new_event[user]["new_uids"].append(data["uids"][0])
                self.new_event[user].update(
                    {
                        "event": event_name,
                        "event_timestamp": self.get_current_timestamp(),
                        "user": user,
                        "new_mailbox": data["mailbox"],
                    }
                )
            elif data["event"] == "MessageExpunge":
                self.new_event[user].update(
                    {
                        "old_uids": data["uids"],
                        "old_mailbox": data["mailbox"],
                        "offset": data["offset"],
                        "topic": data["topic"],
                        "partition": data["partition"],
                    }
                )
                self.set_priority(self.new_event[user])
            return None

    def handle_event(self, event):
        data = event.value
        logger.info(f"DATA {data}")
        if not data:
            return
        if data["event"] in [
            "MessageRead",
            "MailboxSubscribe",
            "MailboxUnsubscribe",
        ]:
            return

        data.update(
            {
                "topic": event.topic,
                "partition": event.partition,
                "offset": event.offset,
            }
        )

        logger.info(f"New event ==> {data['event']}")
        if data["event"] == "MessageAppend" and data["user"] in data.get(
            "from", ""
        ):
            return self.set_priority(data)
        if data["event"] in ["MessageAppend", "MessageExpunge"]:
            try:
                self.custom_event("MessageMove", data)
                return
            except Exception:
                return
        return self.set_priority(data)

    def aggregate_event_by_amount(self):
        start = time.time()
        while True:
            if time.time() - start > WorkerConfig.WINDOW_DURATION:
                self.mqtt.ordered_message(self.user_events)
                self.mqtt.publish_message(self.consumer.consumer)
                self.user_events.clear()
                self.new_event.clear()
                self.messages.clear()
                start = time.time()
            else:
                msg = self.consumer.poll_message()
                if not msg:
                    logger.info("poll timeout")
                    continue
                start = time.time()
                for event in list(msg.values())[0]:
                    self.handle_event(event)
