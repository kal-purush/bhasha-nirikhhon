import node
import enum
import asyio
import asyncio
import functools
import collections
from interface import Interface

__all__ = ["Botnet", "ControlCentre"]

class Permisson(enum.IntEnum):
    NONE = 0
    WORKER = 1
    MAX = 255

class Botnet:

    class Client(node.Client):
        async def dsptch_a(self, data: node.Data):
            pass

    def __init__(self, addr: str, port: int):
        self.connection = self.Client(addr, port)
        self.authority = Permisson.NONE

    def __enter__(self):
        raise NotImplementedError
        return self
    async def __aenter__(self):
        await self.connection.__aenter__()
        return self
    async def __aexit__(self, *args):
        await self.connection.__aexit__(*args)

    async def create_job(self, program, files, save=False) -> int:
        PREFIX = "create_job_"
        self.connection.send(program, PREFIX[:-1])
        jid = (await self.connection.recv(f"{PREFIX}id"))[0].data
        jtag = node.Tag(jid)
        self.connection.send({
            "file_count": len(files),
            "save": save,
        }, f"{PREFIX}meta", str(jid), jtag)
        for file in files:
            self.connection.send(file, f"{PREFIX}file", jtag)
        await self.connection.recv(f"{PREFIX}done", jtag)
        return jid

    async def feed(self, job: int, *data, wait=False):
        for item in data:
            self.connection.send(item, "FEED", node.Tag(job))

class ControlCentre:

    class SClient(node.SClient):
        @property
        def cnc(self) -> 'ControlCentre':
            return self.server.cnc

        async def open(self):
            await super().open()
            config = await self.setup_config()
            self.cnc.add_worker(self, **config)
        async def close(self):
            addr = self.server.connections[self]
            await super().close()
            # print(addr)

        async def setup_config(self) -> dict:
            PREFIX = "setup_config_"
            config: dict = await self.recv(PREFIX[:-1])



        async def distribute_data(self, data: node.Data):
            """Send Data to all Connections"""
            await Interface.gather(*(
                con.send(data)
                for con in self.server.connections
            ))

        async def dsptch_kill(self, data: node.Data):
            """Kill the Control Server"""
            await self.cnc.kill()
            return True
        async def dsptch_cmd(self, data: node.Data):
            print("CMD Input:", type(data.data), data)
            await Interface.gather(*(con.send(data) for con in (c for c in self.server.connections if c is not self)))
        async def dsptch_feed(self, data: node.Data):
            """Distribute Feed Data across network"""
            self.cnc.feed(int(data.tag[0]), data.data)
            return True
        async def dsptch_create_job(self, data: node.Data):
            """Create Job to Distribute Program to Network ready for Data Feed"""
            PREFIX = "create_job_"
            jid = self.cnc.unique()
            jtag = node.Tag(jid)
            self.send(jid, f"{PREFIX}id", jtag)
            meta_data: dict = (await self.recv(f"{PREFIX}meta", jtag))[0].data
            files = await self.recv(f"{PREFIX}file", jtag, wait=meta_data["file_count"])
            await self.cnc.create_job(jid, data.data, files, save=meta_data.get("save", False))
            await self.send(True, f"{PREFIX}done", jtag)
            return True
        async def dsptch_finish_job(self, data: node.Data):
            """Finish Job"""
            await self.cnc.finish_job(data.data)
            return True

    class Worker:
        ACTIVE = 3
        def __init__(self, cnc: 'ControlCentre', connection: 'ControlCentre.SClient'):
            self.connection = connection
            self.queue: list[ControlCentre.Item] = collections.deque()
            self.semaphore = asyncio.BoundedSemaphore(self.ACTIVE)
            self.jobs: ControlCentre.Job = set()

        async def process(self, item: 'ControlCentre.Item'):
            # Send to client
            pass

        async def load_job(self, job: 'ControlCentre.Job'):
            pass

    class Job:
        def __init__(self, id, program):
            self.id = id
            self.program = program
            self.active: int = 0
            self.count: int = 0

            self.done = asyncio.Queue()

    class Item:
        def __init__(self, job: 'ControlCentre.Job', data):
            self.id = job.count
            self.job = job
            self.data = data

            self.job.count += 1
            self.job.active += 1

    @staticmethod
    def Authorise(level: Permisson):
        def auth_decorator(func):
            @functools.wraps(func)
            def auth_wrapper(bot: Botnet, *args, **kwargs):
                if bot.authority < level:
                    raise ValueError(f"{level} required: {bot.authority}")
                return func(bot, *args, **kwargs)
            return auth_wrapper
        return auth_decorator

    def __init__(self, port: int):
        self.server = node.Server("", port, client=self.SClient, echo=node.dispatch.echo)
        self.server.cnc = self
        self.jobs: dict[int, ControlCentre.Job] = {}
        self._unique_ids = set()
        self.manager = asyio.MultiQueue()

    async def create_job(self, jid: int, program, files: list, save=False):
        job = self.jobs[jid] = self.Job(jid, program)
        self.manager.create(job, asyncio.Queue())
        return jid

    async def finish_job(self, jid: int):
        job: ControlCentre.Job = self.jobs[jid]
        que = self.manager[job]
        await que.join()
        self.manager.delete(job)
        # if not save:
        del self.jobs[jid]
        self.unique(jid)

    def feed(self, jid: int, data):
        job: ControlCentre.Job = self.jobs[jid]
        self.manager[job].put_nowait(self.Item(
            job, data
        ))

    async def add_worker(self):
        pass

    async def process_item(self, worker: 'ControlCentre.Worker'):
        while True:
            async with worker.semaphore:
                item: ControlCentre.Item = await self.manager.get(*worker.jobs)
                try:
                    data = await worker.process(item)
                    item.job.done.put_nowait(item)
                except ConnectionError as e:
                    # Connection Closed
                    # Append unfinished items
                    return

    def unique(self, uid=None) -> int:
        if uid is not None:
            self._unique_ids.discard(uid)
            return
        for i in range(2**16):
            if i not in self._unique_ids:
                self._unique_ids.add(i)
                return i
        raise ValueError("Max Unique ID's Reached")

    async def kill(self):
        self.server.close()

    def __enter__(self):
        raise NotImplementedError
        return self
    async def __aenter__(self):
        flag = bool(self.server)
        await self.server.__aenter__()
        if not flag:
            pass
        return self
    async def __aexit__(self, *args):
        await self.server.__aexit__(*args)

class Program:

    def __init__(self):
        pass

    def __getstate__(self) -> dict:
        return {}
    def __setstate__(self, state: dict):
        pass

async def establish_communism(size: int):
    f = Interface.gather(*(Interface.schedule(worker()) for i in range(size)))
    await Interface.next()
    return f

async def worker():
    # print("Worker")
    async with Botnet("127.0.0.1", 51115) as bot:
        print(await bot.connection.recv("cmd"))

async def main():
    async with ControlCentre(51115) as cc:
        async with Botnet("127.0.0.1", 51115) as bot:
            await bot.create_job("my_program", (), save=True)
        await Interface.next(0.1)

    Interface.stop()

if __name__ == "__main__":
    Interface.schedule(main())
    Interface.main()

# import sys
# import os.path
# import modulefinder
# finder = modulefinder.ModuleFinder()
# finder.run_script(sys.argv[0])
# path = [os.path.normpath(f) for f in sys.path[2:]]
# for name, mod in sorted(finder.modules.items()):
#     if mod.__file__:
#         p = os.path.normpath(mod.__file__)
#         if not any(f in p for f in path):
#             print(name, mod.__file__)
# print(path)