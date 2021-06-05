import mail_process_worker.logic.handle_kafka_event as kafka_event
import logging
import calendar
import time
from tests.base import TestLogic
from unittest.mock import patch, MagicMock
from kafka.structs import TopicPartition

logging.disable(logging.CRITICAL)


class TestCustomEvent(TestLogic):
    MESSAGE_FMT = "\nInput: {0!r}\nCorrect result is {1!r} \nReceive {2!r}"

    def test_message_move(self):
        kafka_event.NEW_EVENT = {}
        timestamp = calendar.timegm(time.gmtime())
        events = [
            {
                "user": "user_A",
                "mailbox": "Mailbox_B",
                "event": "MessageAppend",
                "uids": [123],
            },
            {
                "user": "user_A",
                "mailbox": "Mailbox_A",
                "event": "MessageExpunge",
                "uids": [456],
                "topic": "topic-1",
                "partition": 1,
                "offset": 111,
            },
            {
                "user": "user_B",
                "mailbox": "Mailbox_D",
                "event": "MessageAppend",
                "uids": [888],
            },
            {
                "user": "user_B",
                "mailbox": "Mailbox_C",
                "event": "MessageExpunge",
                "uids": [999],
                "topic": "topic-2",
                "partition": 2,
                "offset": 222,
            },
        ]
        expect = {
            "user_A": {
                "new_uids": [123],
                "event": "MessageMove",
                "event_timestamp": timestamp,
                "user": "user_A",
                "new_mailbox": "Mailbox_B",
                "old_uids": [456],
                "old_mailbox": "Mailbox_A",
                "offset": 111,
                "topic": "topic-1",
                "partition": 1,
            },
            "user_B": {
                "new_uids": [888],
                "event": "MessageMove",
                "event_timestamp": timestamp,
                "user": "user_B",
                "new_mailbox": "Mailbox_D",
                "old_uids": [999],
                "old_mailbox": "Mailbox_C",
                "offset": 222,
                "topic": "topic-2",
                "partition": 2,
            },
        }
        for event in events:
            kafka_event.custom_event("MessageMove", event)
        msg = self.MESSAGE_FMT.format(events, expect, kafka_event.NEW_EVENT)
        self.assertEqual(kafka_event.NEW_EVENT, expect, msg)

    def test_set_priority_for_many_user(self):
        kafka_event.USER_EVENTS = {}
        cases = []
        users = [f"user_{numb}" for numb in range(9)]
        events = [
            ("MailboxCreate", 1),
            ("MailboxRename", 2),
            ("MessageNew", 3),
            ("MessageAppend", 4),
            ("FlagsSet", 5),
            ("FlagsClear", 5),
            ("MessageMove", 6),
            ("MessageTrash", 7),
            ("MailboxDelete", 8),
        ]
        for user, event in list(zip(users, events)):
            cases.append(
                (
                    {"user": user, "event": event[0]},
                    {user: [(event[1], {"user": user, "event": event[0]})]},
                )
            )

        for input_data, expect in cases:
            kafka_event.set_priority(input_data)
            msg = self.MESSAGE_FMT.format(
                input_data, expect, kafka_event.USER_EVENTS
            )
            self.assertEqual(kafka_event.USER_EVENTS, expect, msg)
            kafka_event.USER_EVENTS.clear()

    def test_set_priority_for_a_user(self):
        kafka_event.USER_EVENTS = {}
        cases = [
            {"user": "user_A", "event": "MailboxCreate"},
            {"user": "user_B", "event": "MessageNew"},
            {"user": "user_A", "event": "MessageAppend"},
        ]
        for input_data in cases:
            kafka_event.set_priority(input_data)
        expect = {
            "user_A": [
                (1, {"user": "user_A", "event": "MailboxCreate"}),
                (4, {"user": "user_A", "event": "MessageAppend"}),
            ],
            "user_B": [(3, {"user": "user_B", "event": "MessageNew"})],
        }
        msg = self.MESSAGE_FMT.format(
            input_data, expect, kafka_event.USER_EVENTS
        )
        self.assertEqual(kafka_event.USER_EVENTS, expect, msg)

    @patch("mail_process_worker.logic.handle_kafka_event.set_priority")
    def test_handle_event(self, mock_priority):

        cases = []
        events = [
            "MessageNew",
            "MailboxCreate",
            "MailboxRename",
            "MessageAppend",
            "FlagsSet",
            "FlagsClear",
            "MessageTrash",
            "MailboxDelete",
        ]
        for event in events:
            consumer_record = MagicMock(
                topic="topic-1",
                partition="2",
                offset=123,
                value={"event": event, "from": "user_A", "user": "user_A"},
            )
            cases.append((consumer_record, mock_priority()))
        self._test_all(kafka_event.handle_event, cases)

    def test_handle_with_special_event(self):
        cases = []
        events = [
            "MessageRead",
            "MailboxSubscribe",
            "MailboxUnsubscribe",
            "MessageAppend",
            "MessageExpunge",
        ]
        for event in events:
            consumer_record = MagicMock(
                topic="topic-1",
                partition="2",
                offset=123,
                value={"event": event, "from": "", "user": "user_A"},
            )
            cases.append((consumer_record, None))
        self._test_all(kafka_event.handle_event, cases)
