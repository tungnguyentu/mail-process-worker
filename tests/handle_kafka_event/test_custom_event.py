import mail_process_worker.logic.handle_kafka_event as handle_event
import unittest
import logging
import calendar
import time

logging.disable(logging.CRITICAL)


class TestCustomEvent(unittest.TestCase):
    MESSAGE_FMT = "\nInput: {0!r}\nCorrect result is {1!r} \nReceive {2!r}"

    def test_message_move(self):
        handle_event.NEW_EVENT = {}
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
        expect = {'user_A':{
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
        'user_B':{
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
        }}
        for event in events:
            handle_event.custom_event("MessageMove", event)
        msg = self.MESSAGE_FMT.format(events, expect, handle_event.NEW_EVENT)
        self.assertEqual(handle_event.NEW_EVENT, expect, msg)
