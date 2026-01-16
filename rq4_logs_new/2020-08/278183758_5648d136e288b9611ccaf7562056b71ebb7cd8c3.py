from .server import get_app
from fastapi import Request
import httpx
from .config import monitor_url
from datetime import datetime

app = get_app()


@app.middleware("http")
async def send_stats(request: Request, call_next):
    service = request.url.path.split("/")[3]
    start_time = float(datetime.now().timestamp())
    response = await call_next(request)
    process_time = float(datetime.now().timestamp()) - start_time
    url = request.url.path
    status_code = response.status_code
    req = {
        "service": service,
        "url": url,
        "status_code": status_code,
        "response_time": process_time,
        "request_timestamp": start_time,
    }
    async with httpx.AsyncClient() as client:
        await client.post(f"{monitor_url}/api/v1/send/", json=req)

    return response