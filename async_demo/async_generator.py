import asyncio

from async_demo.simulator.AsyncSimulator import async_run
from async_demo.util import getLogger, time_it

LOGGER = getLogger(__name__)


async def sleep_generator(pos: int):
    """An async generator alone won't run the awaitables concurrently"""
    if pos <= 0:
        raise ValueError(f"none_zero must be some positive integer, got {pos}")
    scale = 1 / pos / 10
    for i in range(pos):
        t = scale * (pos - i)
        LOGGER.info(f"Number {i}, going to sleep {t} s")
        await asyncio.sleep(t)
        LOGGER.info(f"Number {i}, slept {t} s")
        yield i


@time_it
@async_run
async def run_generator(pos: int):
    """Async for is just a syntax sugar that matches the regular for in python"""
    LOGGER.info(f"Final sequence: {[i async for i in sleep_generator(pos)]}")
