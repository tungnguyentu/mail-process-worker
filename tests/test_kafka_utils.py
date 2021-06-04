from os import path
from mail_process_worker.logic.kafka_utils import (
    get_topic_partition,
    get_offsets,
)
import logging
from kafka.structs import TopicPartition, OffsetAndTimestamp
from tests.base import TestBase
from unittest.mock import patch, MagicMock

logging.disable(logging.CRITICAL)


class TestTopicPartition(TestBase):
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

    def test_get_event_offsets_with_offset(self):
        consumer = MagicMock()
        data ={
            "offset_start": 1,
            "offset_end": 10,
        }
        expect = 1, 10
        output = get_offsets(data, consumer)
        msg = self.MESSAGE_FMT.format(
            data, expect, output
        )
        self.assertEqual(output, expect, msg)
    
    @patch("mail_process_worker.logic.kafka_utils.get_offset_and_timestamp")
    @patch('mail_process_worker.logic.handle_kafka_event.get_topic_partition')
    def test_get_event_offsets_with_timestamp(self, mock_topic_partition, mock_offset_and_timestamp):
        data = {
            "partition": '1',
            "topic": "topic-1",
            "timestamp_start": "1234567",
            "timestamp_end": "7654321",
        }
        consumer = MagicMock()
        mock_topic_partition.return_value = TopicPartition("topic-1", 1)
        mock_offset_and_timestamp.return_value = OffsetAndTimestamp(offset=1234567, timestamp=0), OffsetAndTimestamp(offset=7654321, timestamp=0)
        expect = 1234567, 7654321
        output = get_offsets(data, consumer)
        msg = self.MESSAGE_FMT.format(
            data, expect, output
        )
        self.assertEqual(output, expect, msg)
        

    def test_send_to_kafka(self):
        pass
