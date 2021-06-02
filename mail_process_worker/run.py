from mail_process_worker.logic.handle_kafka_event import aggregate_event_by_amount
from mail_process_worker.logic.kafka_utils import get_consumer

def main():
	consumer = get_consumer()
	aggregate_event_by_amount(consumer)

if __name__ == '__main__':
	main()