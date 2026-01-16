from time import perf_counter
from typing import Any, Optional
from amas.agent import Agent
from numpy.random import uniform


async def flush_message_for(agent: Agent, duration: float):
    while duration >= 0. and agent.working():
        s = perf_counter()
        await agent.try_recv(duration)
        e = perf_counter()
        duration -= e - s


async def fixed_interval_with_postopone(agent: Agent, duration: float,
                                        target_response: Any, postopone: float = 0.):
    while duration >= 0. and agent.working():
        s = perf_counter()
        mail = await agent.try_recv(duration)
        duration -= perf_counter() - s
        if mail is None:
            duration = 1e-3
            continue
        _, response = mail
        if response != target_response and duration < postopone:
            duration = postopone