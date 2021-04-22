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
    Set,
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


def _batcher(it: Iterable[T], size: int = 1) -> Iterator[T]:
    """Batch generator, sliced by size"""
    stream = iter(it)

    def _slice():
        return list(islice(stream, size))

    yield from iter(_slice, [])


async def _async_generator(it: Iterable[T]) -> AsyncIterator[T]:
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


class AsyncGeneratorSimulator(AsyncSimulator):
    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        """aiostream.stream provides additional iter tools for async generator"""
        await asyncio.gather(
            *[
                self.simulate_IO(i, t)
                async for i, t in aiostream.stream.enumerate(_async_generator(it))
            ]
        )


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
        """Send the iterables in batches, the next batch will be process only when all of the previous batch is done"""
        for batch_idx, batch in enumerate(_batcher(it, self.max_concurrency)):
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

    async def run_queue(self, worker: int, queue: asyncio.Queue):
        while not queue.empty():
            task_no, lag = await queue.get()
            LOGGER.info(f"Worker {worker} get task {task_no} ...")
            await self.simulate_IO(task_no, lag)
            queue.task_done()

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        queue: asyncio.Queue = asyncio.Queue()
        for i, t in enumerate(it):
            LOGGER.info(f"Putting task {i} in the queue ...")
            queue.put_nowait((i, t))
        tasks = [
            asyncio.create_task(self.run_queue(worker, queue))
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
        """Semaphone puts a limit on the number of tasks to be processes concurrently"""
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


@attr.s(auto_attribs=True)
class AsyncFutureSimulator(AsyncSimulator):

    max_concurrency: int = 2

    @async_run
    async def _run(self, it: Iterable[Union[int, float]]):
        """Similar to the usage of concurrent.futures"""
        futures: Set[asyncio.Future] = set()
        for i, t in enumerate(it):
            if len(futures) >= self.max_concurrency:
                futures_done, futures = await asyncio.wait(
                    futures, return_when=asyncio.FIRST_COMPLETED
                )
            futures.add(asyncio.create_task(self.simulate_IO(i, t)))

        # finish the remaining futures
        futures_done, _ = await asyncio.wait(futures)
