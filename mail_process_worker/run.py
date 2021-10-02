import sentry_sdk

from mail_process_worker.logic.handle_kafka_event import HandleEvent

from mail_process_worker.setting import SentryConfig

sentry_sdk.init(SentryConfig.SENTRY_DSN)

def main():
    event = HandleEvent()
    event.aggregate_event_by_amount()


if __name__ == "__main__":
    main()
