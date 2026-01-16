"""Configurations fixed for plugin"""
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class BaseConfiguration:
    """Base configuration class"""
    name: str
    params: dict


@dataclass(frozen=True)
class AlertaConfiguration:
    """Alerta configuration class"""
    config_id: int
    config_name: str
    alerta_endpoint: str
    alerta_timeout: int
    alerta_debug: bool
    skip_environment: str
    repeat_interval: int


@dataclass()
class Blackouts:
    """
    BlackoutStructure class

    :param service: list of services separated by comma
    """
    blackout_id: int
    environment: str
    resource: str
    service: List
    event_name: str
    group_name: str
    tags: str
    start_time: str
    duration: int
    text: str

    def __post_init__(self):
        self.service = self.service.split(',')


@dataclass(frozen=True)
class TopicMap:
    """Topic map class"""
    to: str
    subject: str


def topic_map(topics: list):
    """Topic map"""
    map = {}
    topics = {item[0]: item[1:] for item in topics}
    for key, value in topics.items():
        map.update({key: TopicMap(*value)})
    return map