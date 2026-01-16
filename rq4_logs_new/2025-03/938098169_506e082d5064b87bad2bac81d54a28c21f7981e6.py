from typing import Callable

from pydantic import BaseModel
from typing_extensions import Protocol

from utils import generate_get_config_method


class RedisConfig(BaseModel):
    host: str
    database: int

    def url(self):
        return f"redis://{self.host}:6379/{self.database}"

    @property
    def transport(self):
        return "redis"


def _get_config(_get_value: Callable[[str], str], is_test: bool) -> dict:
    return dict(host=_get_value("REDIS_HOST"), database=_get_value("REDIS_DATABASE"))


class GetRedisConfigCallable(Protocol):
    def __call__(self, yaml_file: str = "app.yaml", **defaults) -> RedisConfig:
        ...


get_broker_config: GetRedisConfigCallable = generate_get_config_method(
    RedisConfig, _get_config
)