import enum
import json
import asyncio
import datetime
import http.client
import urllib.request
from bitfield import BitField
from interface import Interface
from collections import defaultdict
from typing import Iterable, Union, Any

def Address(obj) -> str:
    if isinstance(obj, type): # Root
        return {
            Light: "lights/",
            Group: "groups/",
            Scene: "scenes/",
            Schedule: "schedules/",
        }[obj]
    elif isinstance(obj, BridgeRequest):
        return Address(obj.__class__) + f"{obj.id}/"

class BridgeRequest:

    def __init__(self, bridge: 'Bridge', *params, readonly: set=()):
        self._params = {k: getattr(self.__class__, k) for k in [*params, *readonly]}
        self.bridge = bridge
        self._edits = set()
        self._read_only = set(readonly)
        # self.__fut = Interface.schedule(self.read())
        self.transition: int = None

    def __await__(self):
        return self.sync().__await__()
    async def sync(self, wait=False):
        # await self.__fut
        await self.write()
        await self.read()
        if wait and self.transition is not None:
            await Interface.next(self.transition)
        return self

    async def read(self, data=None):
        await self._edited(True)
        self._params.update(await self._recv((await self._recv_req()) if data is None else data, *self._params.keys()))
    async def write(self, wait=False):
        edit = await self._edited(True)
        if edit:
            params = {k: self._params[k] for k in edit}
            await self._send_req(await self._send(**params))
        if wait and self.transition is not None:
            await Interface.next(self.transition)

    async def _edited(self, clear=False) -> set:
        if clear:
            e = self._edits.copy()
            self._edits.clear()
            return e
        return self._edits

    async def _recv(self, data: dict, *params: str) -> dict:
        return {}
    async def _send(self, **params) -> dict:
        return

    async def _recv_req(self) -> dict:
        return {}
    async def _send_req(self, data):
        return {}

    def __getattribute__(self, key: str):
        if key.startswith("_"):
            try:
                return super().__getattribute__(key)
            except AttributeError:
                if key == "_params":
                    return {}
                raise
        try:
            return self._params[key]
        except KeyError:
            return super().__getattribute__(key)
    def __setattr__(self, key: str, value):
        try:
            if self._params[key] != value:
                if key in self._read_only:
                    return
                self._edits.add(key)
            self._params[key] = value
        except KeyError:
            return super().__setattr__(key, value)

class BridgeIDLookup(type):

    def __call__(self, bridge: 'Bridge', id: int, *args, **kwargs):
        if id is not None:
            obj = bridge.lookup[self].get(id)
            if obj is None:
                obj = super().__call__(bridge, id, *args, **kwargs)
                bridge.lookup[self][id] = obj
            return obj
        return super().__call__(bridge, id, *args, **kwargs)

class Status:
    class Effect(enum.Enum):
        NONE = "none"
        LOOP = "colorloop"

class Light(BridgeRequest, metaclass=BridgeIDLookup):

    _attrs = {
        "brightness": "bri",
        "saturation": "sat",
        "temperature": "ct",
    }

    def __init__(self, bridge: 'Bridge', id: int):
        super().__init__(bridge, "on", "name", "brightness", "hue", "saturation", "temperature", "effect", readonly=["reachable"])
        self.id = id
        self.transition: int = None

    async def _recv(self, data: dict, *params: str) -> dict:
        out = {}
        for attr in params:
            try:
                val = None
                battr = self._attrs.get(attr, attr)
                if attr in ("name",):
                    val = data[battr]
                else:
                    val = data["state"][battr]
                if attr in ("effect",):
                    val = {"effect": Status.Effect}[attr](val)
                out[attr] = val
            except KeyError:    continue
        return out

    async def _send(self, **params) -> dict:
        for k,v in params.items():
            if isinstance(v, enum.Enum):
                params[k] = v.value
        buffer = {"": {}, "state": {}}
        if self.transition is not None and "on" not in params:
            buffer[""]["transitiontime"] = buffer["state"]["transitiontime"] = int(self.transition * 10)
        for key, value in params.items():
            attr = self._attrs.get(key, key)
            if key in ("name",):
                buffer[""][attr] = value
            else:
                buffer["state"][attr] = value
        return buffer

    async def _recv_req(self):
        return await self.bridge.request("GET", f"lights/{self.id}")
    async def _send_req(self, data: dict):
        return await Interface.gather(*(self.bridge.request("PUT", f"lights/{self.id}/{k}", v) for k,v in data.items() if v))

    on: bool = False
    reachable: bool = False
    name: str = "name"
    brightness: int = 0
    hue: int = 0
    saturation: int = 0
    temperature: int = 0
    effect: Status.Effect = Status.Effect.NONE

class Group(BridgeRequest, metaclass=BridgeIDLookup):

    _attrs = {
        "brightness": "bri",
        "saturation": "sat",
        "temperature": "ct",
        "all": "all_on",
        "any": "any_on",
    }

    def __init__(self, bridge: 'Bridge', id: int):
        super().__init__(bridge, "name", "lights", "brightness", "hue", "saturation", "temperature", "effect", readonly=("all", "any"))
        self.id = id
        if not self.id: # id 0 is not editable
            async def lmbd(*args, **kwargs):    return
            self._send_req = lmbd
        self.transition: int = None

    async def _recv(self, data: dict, *params: str) -> dict:
        out = {}
        lights = {}
        for attr in params:
            try:
                light = False
                val = None
                battr = self._attrs.get(attr, attr)
                if attr in ("name", "lights"):
                    if attr == "lights":
                        val = {int(i): Light(self.bridge, int(i)) for i in sorted(data[battr])}
                    else:
                        val = data[battr]
                elif attr in ("all", "any"):
                    val = data["state"][battr]
                else:
                    val = data["action"][battr]
                    light = True
                if attr in ("effect",):
                    val = {"effect": Status.Effect}[attr](val)
                out[attr] = val
                if light:
                    lights[attr] = val
            except KeyError:    continue
        if lights:
            for light in (out.get("lights") or self.lights).values():
                light._params.update(lights)
        return out

    async def _send(self, **params):
        light_data = {k:v for k,v in params.items() if k in ("brightness", "hue", "saturation", "temperature", "effect")}
        if light_data:
            for light in self.lights.values():
                light._params.update()
        for k,v in params.items():
            if isinstance(v, enum.Enum):
                params[k] = v.value
            elif k == "lights":
                params[k] = [l.id for l in v.values()]
        buffer = {"": {}, "action": {}, "state": {}}
        if self.transition is not None:
            buffer[""]["transitiontime"] = buffer["action"]["transitiontime"] = buffer["state"]["transitiontime"] = int(self.transition * 10)
        for key, value in params.items():
            attr = self._attrs.get(key, key)
            if key in ("name", "lights"):
                buffer[""][attr] = value
            elif key in ("all", "any"):
                buffer["state"][attr] = value
            else:
                buffer["action"][attr] = value
        return buffer

    async def _recv_req(self) -> dict:
        return await self.bridge.request("GET", f"groups/{self.id}")
    async def _send_req(self, data: dict):
        return await Interface.gather(*(self.bridge.request("PUT", f"groups/{self.id}/{k}", v) for k,v in data.items() if v))

    @classmethod
    async def Create(cls, bridge: 'Bridge', name: str, *lights: Light) -> 'Group':
        data = {
            "name": name,
            "lights": [str(light.id)if isinstance(light, Light) else str(light) for light in lights]
        }
        response = await bridge.request("POST", "groups", data)
        try:
            group = cls(bridge, int(response[0]["success"]["id"]))
        except KeyError:
            raise RuntimeError(f"Failed Creating {cls.__qualname__}: {response}")
        await group.read(data)
        return group

    async def delete(self) -> bool:
        response = await self.bridge.request("DELETE", f"groups/{self.id}")
        return "success" in response[0]

    name: str = "name"
    lights: dict[int, Light] = {}
    brightness: int = 0
    hue: int = 0
    saturation: int = 0
    temperature: int = 0
    effect: Status.Effect = Status.Effect.NONE
    all: bool = False
    any: bool = False

class Scene(BridgeRequest, metaclass=BridgeIDLookup):
    def __init__(self, bridge: 'Bridge', id: str):
        super().__init__(bridge, "name", "group", "lights", "states")
        self.id = id

    async def _edited(self, clear=False) -> set:
        for i in self.states.values():
            if await i._edited():
                return (await super()._edited(clear)) | {"states",}
        return await super()._edited(clear)

    async def _recv(self, data: dict, *params: str) -> dict:
        out = {}
        for key in params:
            if key == "name":
                out[key] = data[key]
            elif key == "group":
                out[key] = Group(self.bridge, int(data.get(key, 0)))
            elif key == "lights":
                out[key] = {int(l): Light(self.bridge, int(l)) for l in data[key]}
            elif key == "states":
                out[key] = data["lightstates"]
        if "states" in out:
            lights = {}
            for l, data in out["states"].items():
                light = lights[int(l)] = Light(self.bridge, f"S.{self.id}.{l}")
                light.id = int(l)
                for k in light._params:
                    setattr(light, k, None)
                if "transitiontime" in data:
                    light.transition = int(data["transitiontime"]) / 10
                await light.read({"state": data})
            out["states"] = lights
        return out

    async def _send(self, **params) -> dict:
        buffer = {"": {}, "lightstates": {}}
        for key, value in params.items():
            if key == "states":
                for light in value.values():
                    if await light._edited(True):
                        buffer["lightstates"][str(light.id)] = (await light._send(**{k: v for k,v in light._params.items() if v is not None and k not in light._read_only}))["state"]
                        if light.transition is not None:
                            buffer["lightstates"][str(light.id)]["transitiontime"] = int(light.transition * 10)
            elif key == "group":
                pass
                # TODO: Change group
            elif key == "lights":
                pass
                # TODO: Change group
            else:
                buffer[""][key] = value
        return buffer

    async def _recv_req(self) -> dict:
        return await self.bridge.request("GET", f"scenes/{self.id}")

    async def _send_req(self, data: dict):
        states = data.pop("lightstates", {})
        return await Interface.gather(*(self.bridge.request("PUT", f"scenes/{self.id}/{k}", v) for k,v in data.items() if v), *(self.bridge.request("PUT", f"scenes/{self.id}/lightstates/{k}", v) for k,v in states.items() if v))

    async def recall(self):
        d = {"scene": self.id}
        if self.transition is not None:
            d["transitiontime"] = int(self.transition * 10)
        return await self.bridge.request("PUT", f"groups/0/action", data=d)

    @classmethod
    async def Create(cls, bridge: 'Bridge', name: str, *lights: Light) -> 'Scene':
        if not lights:
            raise ValueError("Need Lights")
        data = {"name": name, "recycle": True}
        if isinstance(lights[0], Group):
            data["type"] = "GroupScene"
            data["group"] = str(lights[0].id)
        else:
            data["lights"] = [str(light.id)if isinstance(light, Light) else str(light) for light in lights],
        response = await bridge.request("POST", "scenes", data)
        try:
            scene = cls(bridge, response[0]["success"]["id"])
        except KeyError:
            raise RuntimeError(f"Failed Creating {cls.__qualname__}: {response}")
        await scene.read()
        return scene

    async def delete(self) -> bool:
        response = await self.bridge.request("DELETE", f"scenes/{self.id}")
        return "success" in response[0]

    name: str = "name"
    group: Group = Group
    lights: dict[int, Light] = {}
    states: dict[int, Light] = {}

class Condition:

    class Operator(enum.Enum):
        EQ = Equal = "eq"
        GT = Greater = "gt"
        LT = Less = "lt"
        DX = Delta = Change = "dx"
        Stable = Const = "stable"
        NotStable = NotConst = "not stable"
        IN = "in"
        NotIN = NIN = "not in"
    OP = Operator

    def __init__(self, address: str, oper: Operator, value):
        self.address = address
        self.operator = oper
        self.value = value

    def format(self, bridge: 'Bridge') -> dict:
        out = {
            "address": str(self.address),
            "operator": self.operator.value,
        }
        if self.value is not None:
            out["value"] = self.value
        return out

class Command:

    class Method(enum.Enum):
        GET = "GET"
        POST = CREATE = "POST"
        PUT = UPDATE = "PUT"
        DELETE = "DELETE"

    def __init__(self, address: str, body: dict, method=Method.PUT):
        self.body = body
        self.address = address
        self.method: self.Method = method if isinstance(method, self.Method) else self.Method(method)

    def format(self, bridge: 'Bridge') -> dict:
        return {
            "address": f"/api/{bridge.api_key}/{self.address}",
            "method": self.method.value,
            "body": self.body
        }

    @classmethod
    def Parse(cls, cmd: dict) -> 'Command':
        return cls(cmd["address"].split("/", 3)[-1], cmd["body"], cmd["method"])

    @classmethod
    async def Create_light_state(cls, light: Light, **params) -> 'Command':
        data = (await light._send(**params))["state"]
        return cls(Address(light)+"state", data)
    @classmethod
    async def Create_scene_set(cls, scene: Scene) -> 'Command':
        return cls(Address(scene.group)+"action", {"scene": scene.id})
    # @classmethod
    # async def create_scene_set(cls, scene: Scene) -> 'Command':
    #     return cls(Address(scene.group)+"action", {"scene": scene.id})

class Time:

    class Time:
        _FMT = "%H:%M:%S"
        def __init__(self, time: datetime.time, random: datetime.time=None):
            self.time = time
            self.random = random
        def _fmt_req(self) -> str:
            return "T" + self.time.strftime(self._FMT) + "" if self.random is None else ("A" + self.random.strftime(self._FMT))

    class Weekly(Time, BitField):
        def __init__(self, time: datetime.time, random: datetime.time=None, mon=False, tue=False, wed=False, thu=False, fri=False, sat=False, sun=False):
            super().__init__(time, random)
            for k,v in zip(("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"), (mon, tue, wed, thu, fri, sat, sun)):
                if v:
                    setattr(self, k, v)

        def _fmt_req(self) -> str:
            return "W" + format(self.value(), "03") + "/" + super()._fmt_req()

        sunday = False
        saturday = False
        friday = False
        thursday = False
        wednesday = False
        tuesday = False
        monday = False

    class Timer(Time):
        def _fmt_req(self) -> str:
            return "P" + super()._fmt_req()

    class Recurring(Time):
        def __init__(self, time: datetime.time, random: datetime.time=None, iter: int=None):
            super().__init__(time, random)
            self.iter = iter

        def _fmt_req(self) -> str:
            return "R" + "" if self.iter is None else str(min(max(self.iter, 0), 99)) + "/" + super()._fmt_req()

    class Date(Time):
        _DFMT = "%Y:%m:%d"
        def __init__(self, date: datetime.date, time: datetime.time, random: datetime.time=None):
            self.date = date
            super().__init__(time, random)
        def _fmt_req(self) -> str:
            return self.date.strftime(self._DFMT) + super()._fmt_req()

    def Parse(data: str) -> Union[Time, Date, Weekly, Timer, Recurring]:
        if not data:
            return Time.Time(datetime.time())
        if data.startswith("T"):
            return Time.Time(
                datetime.datetime.strptime(data[1:], Time.Time._FMT).time(),
                datetime.datetime.strptime(data[11:], Time.Time._FMT).time() if "A" in data else None
            )
        else:
            t = Time.Parse(data[data.index("T"):])

        if data.startswith("W"): # Week
            week = Time.Weekly(t.time, t.random)
            week.bit_parse(int(data[1:4]))
            return week
        elif data.startswith("R"): # Recurring Timer
            return Time.Recurring(t.time, t.random, None if data[1] == "/" else int(data[1:3]))
        elif data.startswith("P"): # Timer
            return Time.Timer(t.time, t.random)
        else: # Date
            return Time.Date(datetime.datetime.strptime(data[:data.index("T")], Time.Date._DFMT).date(), t.time, t.random)

class Schedule(BridgeRequest, metaclass=BridgeIDLookup):

    def __init__(self, bridge: 'Bridge', id: int):
        super().__init__(bridge, "name", "desc", "status", "time", "command")
        self.id = id
        self._cache_time = self.time._fmt_req()
        self._cache_cmd = json.dumps(self.command.format(self.bridge))

    @classmethod
    async def Create(cls, bridge: 'Bridge', name: str, time: Time.Time, command: Command, delete=True) -> 'Schedule':
        data = {
            "name": name,
            "localtime": time._fmt_req(),
            "command": command.format(bridge),
        }
        if isinstance(time, (Time.Timer, Time.Date, Time.Recurring)):
            data["autodelete"] = delete
        response = await bridge.request("POST", "schedules", data)
        try:
            obj = cls(bridge, int(response[0]["success"]["id"]))
        except KeyError:
            raise RuntimeError(f"Failed Creating {cls.__qualname__}: {response}")
        obj.name = name
        obj.command = command
        obj._cache_cmd = json.dumps(command.format(bridge))
        obj.time = time
        obj._cache_time = time._fmt_req()
        await obj._edited(True)
        return obj

    async def _edited(self, clear=False) -> set:
        extra = set()
        if json.dumps(self.command.format(self.bridge)) != self._cache_cmd:
             extra.add("command")
        if self.time._fmt_req() != self._cache_time:
             extra.add("time")
        return (await super()._edited(clear)) | extra

    async def _recv(self, data: dict, *params: str) -> dict:
        out = {}
        for attr in params:
            try:
                if attr == "desc":
                    out[attr] = data["description"]
                elif attr == "status":
                    out[attr] = data[attr] == "enabled"
                elif attr == "time":
                    out[attr] = Time.Parse(data["localtime"])
                    out["_cache_time"] = data["localtime"]
                elif attr == "command":
                    out[attr] = Command.Parse(data[attr])
                    out["_cache_cmd"] = json.dumps(out[attr].format(self.bridge))
                else:
                    out[attr] = data[attr]
            except KeyError:    continue
            except TypeError:
                raise RuntimeError(f"Failed Receiving {cls.__qualname__}: {data}")
        return out

    async def _send(self, **params) -> dict:
        buffer = {}
        for key, value in params.items():
            if key == "command":
                buffer[key] = value.format(self.bridge)
            elif key == "time":
                buffer["localtime"] = value._fmt_req()
            elif key == "desc":
                buffer["description"] = value
            elif key == "status":
                buffer[key] = "enabled" if value else "disabled"
            else:
                buffer[key] = value
        return buffer

    async def _recv_req(self) -> dict:
        return await self.bridge.request("GET", f"schedules/{self.id}")
    async def _send_req(self, data: dict):
        return await self.bridge.request("PUT", f"schedules/{self.id}", data)

    async def delete(self) -> bool:
        response = await self.bridge.request("DELETE", f"schedules/{self.id}")
        return "success" in response[0]

    name: str = "name"
    desc: str = "description"
    status: bool = True
    time: Union[Time.Time, Time.Date, Time.Weekly, Time.Timer, Time.Recurring] = Time.Time(datetime.time())
    command: Command = Command("", {})

class Sensor:

    class Info(BridgeRequest, metaclass=BridgeIDLookup):

        _name_i = ""
        _attrs = {}

        def __init__(self, bridge: 'Bridge', id: int, *params: str):
            bridge.lookup[Sensor][id] = self
            super().__init__(bridge, "name", "type", "on", *params, readonly={"reachable",})
            self.id = id

        name: str = "name"
        type: str = "type"
        on: bool = True
        reachable: bool = True

        async def _recv(self, data: dict, *params) -> dict:
            out = {}
            for key in params:
                try:
                    if key in ("name", "type"):
                        out[key] = data[key]
                    elif key in ("on", "reachable"):
                        out[key] = data["config"][key]
                    else:
                        out[key] = data["state"][self._attrs.get(key, key)]
                except KeyError:    continue
            return out

        async def _send(self, **params) -> dict:
            buffer = {"": {}, "config": {}, "state": {}}
            for key, value in params.items():
                if key in ("name", "type"):
                    buffer[""][key] = value
                elif key in ("on", "reachable"):
                    buffer["config"][key] = value
                else:
                    buffer["state"][self._attrs.get(key, key)] = value
            return buffer

        async def _recv_req(self) -> dict:
            return await self.bridge.request("GET", f"sensors/{self.id}")
        async def _send_req(self, data: dict) -> dict:
            return Interface.gather(*(self.bridge.request("PUT", f"sensors/{self.id}/{k}", v) for k,v in data.items() if v))

        @classmethod
        async def Create(cls, bridge: 'Bridge', name: str) -> 'Schedule':
            cls._name_i
            data = {
                "name": name,
                "type": cls._name_i,
                "command": command.format(bridge),
                "modelid": f"hue.{cls.__name__}",
                "swversion": "1.0",
                "uniqueid": "1",
                "manufacturername": "hue",
            }
            if isinstance(time, (Time.Timer, Time.Date, Time.Recurring)):
                data["autodelete"] = delete
            response = await bridge.request("POST", "schedules", data)
            try:
                obj = cls(bridge, int(response[0]["success"]["id"]))
            except KeyError:
                raise RuntimeError(f"Failed Creating {cls.__qualname__}: {response}")
            obj.name = name
            obj.command = command
            obj._cache_cmd = json.dumps(command.format(bridge))
            obj.time = time
            obj._cache_time = time._fmt_req()
            await obj._edited(True)
            return obj

    class Flag(Info):

        _name_i = "CLIPGenericFlag"

        def __init__(self, bridge: 'Bridge', id: int):
            super().__init__(bridge, id, "flag")

        flag: bool = False

    class Status(Info):

        _name_i = "CLIPGenericStatus"

        def __init__(self, bridge: 'Bridge', id: int):
            super().__init__(bridge, id, "status")

        status: int = 0

    @classmethod
    def Parse(cls, bridge, data: dict) -> type[Union[Info, Flag, Status]]:
        stype = data["type"]
        lkup = {
            "flag": cls.Flag,
            "status": cls.Status,
        }
        return lkup.get(stype.lower(), cls.Info)

class Bridge:

    class __Con:
        def __init__(self, connection: http.client.HTTPConnection, sem: asyncio.Semaphore):
            self._ready = True
            self.con = connection
            self._sem = sem
        def __bool__(self) -> bool:
            return self._ready
        def acquire(self):
            self._ready = False
            return self
        def release(self):
            self._ready = True
            self._sem.release()

    def __init__(self, addr: str, port: int, api: str, requests: int=10):
        self.addr, self.port = addr, port
        self.api_key = api
        self._sem = asyncio.Semaphore(requests)
        self._conns = tuple(self.__Con(http.client.HTTPConnection(self.addr, self.port, timeout=5), self._sem) for i in range(requests))
        self.lookup: dict[type, BridgeRequest] = defaultdict(dict)
        Interface.terminate.schedule(lambda: [c.con.close() for c in self._conns])

    def __await__(self):
        return self.sync().__await__()

    async def sync(self):
        await Interface.gather(self._load_lights(), self._load_groups(), self._load_scenes(), self._load_schedules())

    async def _get_connection(self) -> __Con:
        await self._sem.acquire()
        for i, con in enumerate(self._conns):
            if con:
                return con.acquire()

    async def request(self, method: str, cmd: str, data: dict=None) -> dict:
        if data:
            data = json.dumps(data)

        con = await self._get_connection()
        data = await Interface.process(self.__req, con.con, method, f"/api/{self.api_key}/{cmd}", data)
        con.release()
        return data

    def __req(self, connection: http.client.HTTPConnection, method: str, url: str, body=None):
        # print("Request:", method, url, body)
        connection.request(method, url, body=body)
        res = connection.getresponse()
        return json.loads(res.read())

    def lookup_name(self, type: type, name: str):
        try:
            for obj in self.lookup[type].values():
                if obj.name == name:
                    return obj
            raise KeyError(f"'{name}' does not exist")
        except AttributeError:
            raise AttributeError(f"{type.__qualname__} has no name")

    async def _load_lights(self) -> Iterable[Light]:
        data = await self.request("GET", "groups/0")
        await Interface.gather(*(Light(self, int(index)) for index in data["lights"]))
        return self.lookup[Light].values()
    async def _load_groups(self) -> Iterable[Group]:
        data = await self.request("GET", "groups")
        await Interface.gather(*(Group(self, int(index)).read(group) for index, group in data.items()))
        return self.lookup[Group].values()
    async def _load_scenes(self) -> Iterable[Scene]:
        data = await self.request("GET", "scenes")
        await Interface.gather(*(Scene(self, index) for index in data))
        return self.lookup[Scene].values()
    async def _load_schedules(self) -> Iterable[Schedule]:
        data = await self.request("GET", "schedules")
        await Interface.gather(*(Schedule(self, int(index)).read(payload) for index, payload in data.items()))
        return self.lookup[Schedule].values()