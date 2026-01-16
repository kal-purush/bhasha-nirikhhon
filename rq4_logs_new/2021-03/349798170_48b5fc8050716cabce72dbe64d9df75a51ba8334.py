from aioredis import create_redis_pool
from aioredis.commands import Redis
from fastapi import Request


async def get_redis_from_app(request: Request):
    return request.app.redis


async def get_cache_backend(config) -> Redis:
    connection = await create_redis_pool(config.redis_dsn)
    return connection


def create_redis(app):
    async def startup_hook() -> None:
        app.redis = await get_cache_backend(app.config)

    return startup_hook


def shutdown_redis(app):
    async def shutdown_hook() -> None:
        if app.redis is not None:
            app.redis.close()
            await app.redis.wait_closed()

    return shutdown_hook