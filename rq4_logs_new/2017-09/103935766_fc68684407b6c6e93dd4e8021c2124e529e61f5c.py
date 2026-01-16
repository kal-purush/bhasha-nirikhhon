#!/usr/local/bin/python

from functools import wraps
import errno
import os
import signal

# Entirely stolen from http://stackoverflow.com/questions/2281850

class TimeoutError(Exception):
    pass

class within:
    def __init__(self, ms=1000, error_message='Timeout'):
        self.ms = ms
        self.error_message = error_message

    def handler(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.ms/1000)

    def __exit__(self, type, value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)

def timeout(ms=1000, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, ms/1000)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
            return result

        return wraps(func)(wrapper)

    return decorator
