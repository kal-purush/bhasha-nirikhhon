#!/usr/bin/env python3
"""
    Import async_generator from the previous task and then write a coroutine
    called async_comprehension that takes no arguments.
    The coroutine will collect 10 random numbers using an async comprehensing
    over async_generator, then return the 10 random numbers.
"""
import asyncio
import time

async_comprehension = __import__('1-async_comprehension').async_comprehension


async def measure_runtime() -> float:
    """ measures the runtime of executing async_comprehension() concurrently 4 times """
    tasks = []
    start_time = time.time()
    for i in range(4):
        tasks.append(asyncio.create_task(async_comprehension()))
    await asyncio.gather(*tasks)
    end_time = time.time()
    return end_time - start_time