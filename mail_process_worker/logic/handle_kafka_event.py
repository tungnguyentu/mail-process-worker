import calendar
import time

from kafka.structs import OffsetAndMetadata

from mail_process_worker.utils.logger import logger
from mail_process_worker.utils.decorator import timeout
from mail_process_worker.logic.kafka_utils import (
    send_to_kafka,
    get_topic_partition,
    get_offsets,
)

from mail_process_worker.setting import WorkerConfig

USER_EVENTS = {}

NEW_EVENT = {}

MESSAGES = []


def get_current_timestamp():
    timestamp = calendar.timegm(time.gmtime())
    return timestamp


def set_priority(data: dict):
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
def custom_event(event_name: str, data: dict):
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
            set_priority(NEW_EVENT[user])
        return None


def handle_event(event):
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
        return set_priority(data)
    if data["event"] in ["MessageAppend", "MessageExpunge"]:
        try:
            custom_event("MessageMove", data)
            return
        except Exception:
            return
    return set_priority(data)


def resend(data, consumer):
    offset_start, offset_end = get_offsets(data, consumer)
    tp = get_topic_partition(data)
    resend_user = data["user"]
    try:
        consumer.seek(tp, offset=offset_start)
    except AssertionError:
        return
    while True:
        msg = consumer.poll(10)
        if not msg:
            logger.info("poll timeout")
            continue
        for event in list(msg.values())[0]:
            resend_data = event.value
            if resend_data["event"] == "seek":
                continue
            if resend_data["user"] != resend_user:
                continue
            if event.offset >= offset_end + 1:
                send_to_kafka(consumer, USER_EVENTS)
                USER_EVENTS.clear()
                NEW_EVENT.clear()
                MESSAGES.clear()
                return
            handle_event(event)


def aggregate_event_by_amount(consumer):
    start = time.time()
    while True:
        if (
            len(MESSAGES) == WorkerConfig.NUMBER_OF_MESSAGE
            or time.time() - start > WorkerConfig.WINDOW_DURATION
        ):
            send_to_kafka(consumer, USER_EVENTS)
            USER_EVENTS.clear()
            NEW_EVENT.clear()
            MESSAGES.clear()
            start = time.time()
        else:
            msg = consumer.poll(1000)
            if not msg:
                logger.info("poll timeout")
                continue
            start = time.time()
            for event in list(msg.values())[0]:
                data = event.value
                if data["event"] == "seek":
                    last_offset_commit = event.offset + 1
                    logger.info(f"LAST OFFSET COMMIT {last_offset_commit}")
                    resend(data, consumer)
                    tp = get_topic_partition(data)
                    consumer.seek(tp, last_offset_commit)
                    consumer.commit(
                        {tp: OffsetAndMetadata(last_offset_commit, None)}
                    )
                    continue
                handle_event(event)
