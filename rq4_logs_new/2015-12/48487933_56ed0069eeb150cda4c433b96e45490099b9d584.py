from contextlib import contextmanager
from dnd.configuration import Config
from dnd.rules import Rule
from dnd.locks import Lock

import redis
import datetime

config = None

@contextmanager
def configure(host='localhost', port=6379, db=0):
    global config
    config = Config(host, port, db)
    yield config
    config.rule = config.rule or Rule(config, pattern="%s:r:c:%s:t:%s")
    config.lock = config.lock or Lock(config, pattern="%s:l:c:%s:u:%s:t:%s")
    config.namespace = config.namespace or 'dnd'
    config.cache_size = config.cache_size or 99


@contextmanager
def throttle(channel, user, category):
    rule = config.rule.get(channel, category)
    lock = config.lock.get(channel, user, category)
    now = datetime.datetime.now()
    if lock :
        lock_stamp = datetime.datetime.strptime(lock, "%Y-%m-%d %H:%M:%S.%f")
        if (now - lock_stamp) >= datetime.timedelta(seconds=int(rule)):
            config.lock.add(channel, user, category, now)
            yield True
        else:
            yield False
    else:
        config.lock.add(channel, user, category, now)
        yield True