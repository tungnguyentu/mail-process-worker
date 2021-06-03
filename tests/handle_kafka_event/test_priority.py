import mail_process_worker.logic.handle_kafka_event as handle_event
import unittest
import logging

logging.disable(logging.CRITICAL)


class TestPriority(unittest.TestCase):
    MESSAGE_FMT = "\nInput: {0!r}\nCorrect result is {1!r} \nReceive {2!r}"

    def test_set_priority_for_many_user(self):
        handle_event.USER_EVENTS = {}
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
            handle_event.set_priority(input_data)
            msg = self.MESSAGE_FMT.format(
                input_data, expect, handle_event.USER_EVENTS
            )
            self.assertEqual(handle_event.USER_EVENTS, expect, msg)
            handle_event.USER_EVENTS.clear()

    def test_set_priority_for_a_user(self):
        handle_event.USER_EVENTS = {}
        cases = [
            {"user": "user_A", "event": "MailboxCreate"},
            {"user": "user_B", "event": "MessageNew"},
            {"user": "user_A", "event": "MessageAppend"},
        ]
        for input_data in cases:
            handle_event.set_priority(input_data)
        expect = {
            "user_A": [
                (1, {"user": "user_A", "event": "MailboxCreate"}),
                (4, {"user": "user_A", "event": "MessageAppend"}),
            ],
            "user_B": [(3, {"user": "user_B", "event": "MessageNew"})],
        }
        msg = self.MESSAGE_FMT.format(
            input_data, expect, handle_event.USER_EVENTS
        )
        self.assertEqual(handle_event.USER_EVENTS, expect, msg)
