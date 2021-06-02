from functools import wraps
import errno
import os
import signal
import time


class TimeoutError(Exception):
    pass


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def retry(times=3, delay=1, exceptions=Exception, logger=None):
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    message = (
                        "Retry exception thrown when attempting to run {}, "
                        "attempt {} of {}".format(func, attempt, times)
                    )
                    if logger:
                        logger.info(message)
                    else:
                        print(message)
                    attempt += 1
                    time.sleep(delay)
            return func(*args, **kwargs)

        return newfn

    return decorator
