from os import path
from mail_process_worker.logic.kafka_utils import (
    get_topic_partition,
    get_offsets,
    get_offset_and_timestamp,
)
import logging
from kafka.structs import TopicPartition, OffsetAndTimestamp
from tests.base import TestLogic
from unittest.mock import patch, MagicMock

logging.disable(logging.CRITICAL)


class TestTopicPartition(TestLogic):
    MESSAGE_FMT = "\nInput: {0!r}\nCorrect result is {1!r} \nReceive {2!r}"

    def test_topic_partition(self):
        cases = [
            (
                {"topic": "topic-1", "partition": 0},
                TopicPartition("topic-1", 0),
            ),
            (
                {"topic": "topic-1", "partition": "1"},
                TopicPartition("topic-1", 1),
            ),
        ]
        self._test_all(get_topic_partition, cases)

    @patch("mail_process_worker.logic.kafka_utils.get_offset_and_timestamp")
    @patch("mail_process_worker.logic.handle_kafka_event.get_topic_partition")
    def test_get_offsets(
        self, mock_topic_partition, mock_offset_and_timestamp
    ):
        cases = ({
            "partition": "1",
            "topic": "topic-1",
            "timestamp_start": "1234567",
            "timestamp_end": "7654321",
        }, {
            "offset_start": 1,
            "offset_end": 10,
        })
        consumer = MagicMock()
        mock_topic_partition.return_value = TopicPartition("topic-1", 1)
        mock_offset_and_timestamp.return_value = OffsetAndTimestamp(
            offset=1, timestamp=0
        ), OffsetAndTimestamp(offset=10, timestamp=0)
        expect = 1, 10
        for data in cases:
            output = get_offsets(data, consumer)
            msg = self.MESSAGE_FMT.format(data, expect, output)
            self.assertEqual(output, expect, msg)

    def test_get_offset_and_timestamp(self):
        tp = TopicPartition("topic-1", 1)
        start = OffsetAndTimestamp(offset=1, timestamp=0)
        end = OffsetAndTimestamp(offset=10, timestamp=0)
        cases = {None: None, start: None, None: end, start: end}
        for key, value in cases.items():
            consumer = MagicMock()
            consumer.offsets_for_times.side_effect = [{tp: key}, {tp: value}]
            output = get_offset_and_timestamp(tp, consumer, 123, 456)
            if key is None or value is None:
                expect = None
            else:
                expect = start, end
            msg = self.MESSAGE_FMT.format(
                [{tp: key}, {tp: value}], expect, output
            )
            self.assertEqual(output, expect, msg)
