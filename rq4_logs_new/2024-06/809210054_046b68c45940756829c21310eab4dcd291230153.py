from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
import httpx
import asyncio
from rest_api.controllers.get_token import verify_token
app = FastAPI()


services = {
    "user_service": "http://messenger:8000",

}

# Реализация Circuit Breaker
class CircuitBreaker:
    def __init__(self, max_failures: int, reset_timeout: int):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = None

    async def call(self, service_name: str, endpoint: str):
        if self.state == "OPEN":
            if (self.last_failure_time + self.reset_timeout) < asyncio.get_event_loop().time():
                self.state = "HALF_OPEN"
            else:
                raise HTTPException(status_code=503, detail="Service Unavailable")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{services[service_name]}{endpoint}")
                response.raise_for_status()
                self.failures = 0
                self.state = "CLOSED"
                return response.json()
        except httpx.RequestError as exc:
            self.failures += 1
            if self.failures >= self.max_failures:
                self.state = "OPEN"
                self.last_failure_time = asyncio.get_event_loop().time()
            raise HTTPException(status_code=503, detail="Service Unavailable")

circuit_breakers = {
    "user_service": CircuitBreaker(max_failures=3, reset_timeout=60),
    # Добавьте другие Circuit Breakers для других сервисов здесь
}

@app.get("/proxy/{service_name}/{endpoint:path}")
async def proxy(service_name: str, endpoint: str, token: str = Depends(verify_token)):
    if service_name not in services:
        raise HTTPException(status_code=404, detail="Service Not Found")
    
    cb = circuit_breakers[service_name]
    return await cb.call(service_name, f"/{endpoint}")