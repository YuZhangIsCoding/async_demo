import asyncio
import time
from functools import wraps
from itertools import islice
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    TypeVar,
    Union,
)

import aiostream
import attr

from async_demo.simulator.Simulator import Simulator
from async_demo.util import getLogger, time_it

T = TypeVar("T")
LOGGER = getLogger(__name__)


def async_run(func: Callable[..., Awaitable]) -> Callable:
    """Decorate to execute coroutines"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


def batcher(it: Iterable[T], size: int = 1) -> Iterator[T]:
    """Batch generator, sliced by size"""
    stream = iter(it)

    def _slice():
        return list(islice(stream, size))

    yield from iter(_slice, [])


async def async_generator(it: Iterable[T]) -> AsyncIterator[T]:
    """Make an async generator from an iterable"""
    for i in it:
        yield i


class AsyncSimulator(Simulator):
    async def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        LOGGER.info(
            f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
        )
        await asyncio.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")
        self.count += 1

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        await asyncio.gather(*[self.simulate_IO(i, t) for i, t in enumerate(it)])

    @time_it
    def run(self, it: Iterable[Union[int, float]]) -> int:
        self._run(it)
        LOGGER.debug(f"Final count for `{self.__class__.__name__}`: {self.count}")
        return self.count


class AsyncUnsafeSimulator(AsyncSimulator):
    async def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        LOGGER.info(
            f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
        )
        await asyncio.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")

        LOGGER.debug(f"Updating value for task {task_no}")
        tmp = self.count + 1
        await asyncio.sleep(self.scale)
        self.count = tmp
        LOGGER.debug(f"Count updated for task {task_no} as {self.count}")


class AsyncSafeSimulator(AsyncSimulator):
    async def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        LOGGER.info(
            f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
        )
        await asyncio.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")

        LOGGER.debug(f"Updating value for task {task_no}")
        tmp = self.count + 1
        time.sleep(self.scale)
        self.count = tmp
        LOGGER.debug(f"Count updated for task {task_no} as {self.count}")


@attr.s(auto_attribs=True)
class AsyncBatchSimulator(AsyncSimulator):
    """Using a batch generator to limit the number of concurrent jobs"""

    max_concurrency: int = 2

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        for batch_idx, batch in enumerate(batcher(it, self.max_concurrency)):
            LOGGER.info(f"Processing batch {batch_idx}")
            await asyncio.gather(
                *[
                    self.simulate_IO(i + batch_idx * self.max_concurrency, t)
                    for i, t in enumerate(batch)
                ]
            )


@attr.s(auto_attribs=True)
class AsyncQueueSimulator(AsyncSimulator):
    """Use Queue to limit workers"""

    max_concurrency: int = 2

    async def simulate_IO(self, worker: int, queue: asyncio.Queue, *args, **kwargs):
        """Have the working running items from the queue until the queue is empty"""
        while not queue.empty():
            task_no, lag = await queue.get()
            LOGGER.info(
                f"Worker {worker} starts simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
            )
            await asyncio.sleep(self.scale * lag)
            queue.task_done()
            LOGGER.info(f"Complete simulation of task {task_no}")
            self.count += 1

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        queue = asyncio.Queue()
        for i, t in enumerate(it):
            LOGGER.info(f"Putting task {i} in the queue ...")
            queue.put_nowait((i, t))
        tasks = [
            asyncio.create_task(self.simulate_IO(worker, queue))
            for worker in range(self.max_concurrency)
        ]
        await queue.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks)


@attr.s
class AsyncSemaphoreSimulator(AsyncSimulator):

    max_concurrency = attr.ib(default=2, type=int)
    sem = attr.ib(init=False)

    async def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        async with self.sem:
            LOGGER.info(
                f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
            )
            await asyncio.sleep(self.scale * lag)
            LOGGER.info(f"Complete simulation of task {task_no}")
            self.count += 1

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        self.sem = asyncio.Semaphore(self.max_concurrency)
        await asyncio.gather(*[self.simulate_IO(i, t) for i, t in enumerate(it)])


class AsyncGeneratorSimulator(AsyncSimulator):
    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        await asyncio.gather(
            *[
                self.simulate_IO(i, t)
                async for i, t in aiostream.stream.enumerate(async_generator(it))
            ]
        )
