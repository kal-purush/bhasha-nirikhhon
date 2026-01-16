

from abc import ABC, abstractmethod
from email.policy import default
from typing import Dict, Literal

from aiohttp import ClientSession
from core.types import AntevortaDiscovery, AntevortaDiscoveryStatus


class AntevortaOutputAdapter(ABC):
    @abstractmethod
    async def save(self, discovery: AntevortaDiscovery) -> AntevortaDiscoveryStatus:
        pass


RESTMethod = Literal["POST", "GET"]


class RESTAdapter(AntevortaOutputAdapter):
    def __init__(self, endpoint: str, method: RESTMethod = "POST", remmaping: Dict[str, str] = {}) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.remmaping = remmaping

    async def save(self, discovery: AntevortaDiscovery) -> AntevortaDiscoveryStatus:
        payload = discovery.__dict__

        if len(self.remmaping) > 0:
            for last_key, new_key in self.remmaping.items():
                payload[new_key] = payload.pop(last_key)

        async with ClientSession() as session:
            content, status = "", 0

            match self.method:
                case "POST":
                    async with session.post(self.endpoint) as res:
                        content = await res.text()
                        status = res.status
                case "GET":
                    async with session.get(self.endpoint) as res:
                        content = await res.text()
                        status = res.status

            print(f"{self.endpoint} [{status}] {content}")

            if status == 200:
                return AntevortaDiscoveryStatus.DISCOVERED

        return AntevortaDiscoveryStatus.UNKNOWN