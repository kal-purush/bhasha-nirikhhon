import asyncio
import aioredis
import functools
import json
import os
import sys
import time
import uvloop

from aioredis.errors import ConnectionClosedError


QUEUE = os.environ.get('QUEUE_NAME')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PASS = os.environ.get('REDIS_PASS')
REDIS_PORT = os.environ.get('REDIS_PORT')
REDIS_DB = os.environ.get('REDIS_DB')

PID = os.getpid()
QUEUE_TASK = f'macul:{QUEUE}:task'
QUEUE_FAIL = f'macul:{QUEUE}:fail'


class Macul:

    def __init__(self):
        self.redis = aioredis.create_redis_pool(
            f'redis://{REDIS_HOST}:{REDIS_PORT}',
            password=REDIS_PASS,
            db=int(REDIS_DB),
        )

    def consumer(self):
        def wrapper(func):
            async def wrapped(*args):
                print(f'Worker {PID} is listening on "{QUEUE_TASK}"')
                redis_connection = await self.redis
                while True:
                    _, data = await conn.brpop(QUEUE_TASK)
                    data = json.loads(data.decode('utf8'))
                    try:
                        await func(data)
                    except KeyError:
                        print('invalid payload')
                    except Exception:
                        data = json.dumps(data)
                        redis.lpush(QUEUE_FAIL)
            return wrapped
        return wrapper

    def executor(self, func):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.get_event_loop()
        try:
            asyncio.ensure_future(func())
            event_loop.run_forever()
        except ConnectionClosedError:
            print('Connection closed...')
        except KeyboardInterrupt:
            sys.exit(1)
        finally:
            event_loop.close()

    def __repr__(self):
        return f'<{self.__class__.name}>'