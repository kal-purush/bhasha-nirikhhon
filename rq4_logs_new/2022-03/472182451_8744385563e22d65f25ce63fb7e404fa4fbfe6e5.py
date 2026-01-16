from asyncio import gather
from datetime import datetime
from time import time
from typing import List
from fastapi import FastAPI, Query
from pydantic import BaseModel
from api.semaphore import Semaphore
from core.discover import perform_discovery
from core.types import AntevortaDiscovery

app = FastAPI()


@Semaphore(10)
async def discovery_task(url: str):
    return await perform_discovery(url)


class PerformDiscoveryResult(BaseModel):
    compute_time_ms: float
    error: str | None
    discoveries: List[AntevortaDiscovery] | None


@app.get("/perform-discovery")
async def perform_discovery_endpoint(urls: List[str] | None = Query(None)) -> PerformDiscoveryResult:
    now = datetime.now()
    match urls:
        case None:
            return PerformDiscoveryResult(
                compute_time_ms=(datetime.now() - now).microseconds / 1000,
                error="No URLs provided",
            )
        case urls:
            results = await gather(*[discovery_task(url) for url in urls])
            return PerformDiscoveryResult(
                compute_time_ms=(datetime.now() - now).microseconds / 1000,
                error=None,
                discoveries=results,
            )