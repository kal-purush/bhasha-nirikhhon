"""
An monkeypatched version of ipfsapi which provides async methods
"""
from functools import partial
from types import MethodType
import asyncio
import ipfsapi

async def connect(*args, **kwargs):
    """Mirrors the ipfsapi.connect(), but returns a monkey-patched
    ipfsapi.Client object"""
    connect = partial(ipfsapi.connect, *args, **kwargs)
    api = await asyncio.get_event_loop().run_in_executor(None, connect)

    methods = [attr for attr in dir(api) if (not attr.startswith('_') and
               isinstance(getattr(api, attr), MethodType))]
    
    async def _monkey_patch(method, *args, **kwargs):
        print('_monkey_patch call %s %s %s' % (method, str(args), str(kwargs)))
        ret = await asyncio.get_event_loop().run_in_executor(
            None, partial(method, *args, **kwargs))
        print('_monkey_patch returning %s' % str(ret))
        return ret

    for attr in methods:
        setattr(api, attr, partial(_monkey_patch, getattr(api, attr)))

    return api