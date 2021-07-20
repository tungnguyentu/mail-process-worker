import calendar
import time
import datetime
from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import timeout
from mail_process_worker.logic.kafka_utils import send_to_kafka

from mail_process_worker.setting import WorkerConfig

USER_EVENTS = {}

NEW_EVENT = {}

MESSAGES = []


def get_current_timestamp():
    timestamp = calendar.timegm(time.gmtime())
    return timestamp


def set_priority(data: dict, consumer):
    if len(MESSAGES) == WorkerConfig.NUMBER_OF_MESSAGE:
        send_to_kafka(consumer, USER_EVENTS)
        USER_EVENTS.clear()
        NEW_EVENT.clear()
        MESSAGES.clear()
    MESSAGES.append(data)

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
    exist_user = USER_EVENTS.get(user, None)
    if not exist_user:
        USER_EVENTS.update({user: []})
    USER_EVENTS[user].append((event_priority[event_name], data))
    logger.info(f"set priority for {data['event']} | DONE")


@timeout(10)
def custom_event(event_name: str, data: dict, consumer):
    if event_name == "MessageMove":
        user = data["user"]
        if data["event"] == "MessageAppend":
            exist_user = NEW_EVENT.get(user, None)
            if not exist_user:
                NEW_EVENT.update(
                    {
                        user: {
                            "new_uids": [],
                        }
                    }
                )
            NEW_EVENT[user]["new_uids"].append(data["uids"][0])
            NEW_EVENT[user].update(
                {
                    "event": event_name,
                    "event_timestamp": get_current_timestamp(),
                    "user": user,
                    "new_mailbox": data["mailbox"],
                }
            )
        elif data["event"] == "MessageExpunge":
            NEW_EVENT[user].update(
                {
                    "old_uids": data["uids"],
                    "old_mailbox": data["mailbox"],
                    "offset": data["offset"],
                    "topic": data["topic"],
                    "partition": data["partition"],
                }
            )
            set_priority(NEW_EVENT[user], consumer)
        return None


def handle_event(consumer, event):
    data = event.value
    logger.info(data)
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
        event_timestamp = data["event_timestamp"]
        data.update(
            {
                "date": datetime.datetime.utcfromtimestamp(
                    int(event_timestamp)
                )
                .astimezone()
                .replace(microsecond=0)
                .isoformat()
            }
        )
        return set_priority(data, consumer)
    if data["event"] in ["MessageAppend", "MessageExpunge"]:
        try:
            custom_event("MessageMove", data, consumer)
            return
        except Exception:
            return
    return set_priority(data, consumer)


def aggregate_event_by_amount(consumer):
    start = time.time()
    while True:
        if time.time() - start > WorkerConfig.WINDOW_DURATION:
            send_to_kafka(consumer, USER_EVENTS)
            USER_EVENTS.clear()
            NEW_EVENT.clear()
            MESSAGES.clear()
            start = time.time()
        else:
            msg = consumer.poll(10000)
            if not msg:
                logger.info("poll timeout")
                continue
            start = time.time()
            for event in list(msg.values())[0]:
                handle_event(consumer, event)
