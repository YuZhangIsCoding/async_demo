#!/usr/bin/env python
import click

from .__init__ import __version__
from async_demo.simulator.Simulator import Simulator
from async_demo.simulator.ThreadingSimulator import (
    NaiveThreadingSimulator,
    ThreadingUnsafeSimulator,
    ThreadingLockSimulator,
    ThreadingPoolSimulator,
)
from async_demo.simulator.AsyncSimulator import (
    AsyncUnsafeSimulator,
    AsyncSafeSimulator,
    AsyncSimulator,
    AsyncBatchSimulator,
    AsyncQueueSimulator,
    AsyncSemaphoreSimulator,
    AsyncGeneratorSimulator,
)
from async_demo.util import getLogger

LOGGER = getLogger(__name__)


@click.group()
@click.version_option(version=__version__)
def main():
    pass


@main.command(help="Show the potential race condition in the multi-threading approach ")
@click.option("--n-time", type=click.IntRange(min=1, max=10), default=2, help="Number of times")
def show_race(n_time: int):
    times = [1 for _ in range(n_time)]

    res = []
    for sim_class in [
        ThreadingUnsafeSimulator,
        ThreadingLockSimulator,
        AsyncUnsafeSimulator,
        AsyncSafeSimulator,
    ]:
        LOGGER.info(f"\nRunning {sim_class.__name__} ...")
        sim = sim_class()
        res.append((sim_class.__name__, sim.run(times)))

    for class_name, count in res:
        LOGGER.info(f"`{class_name}` count: {count}")


@main.command(help="Show all methods")
def show_all():
    times = [2, 3, 1]

    for sim_class in [
        Simulator,
        NaiveThreadingSimulator,
        ThreadingLockSimulator,
        ThreadingPoolSimulator,
        AsyncSimulator,
        AsyncBatchSimulator,
        AsyncQueueSimulator,
        AsyncSemaphoreSimulator,
        AsyncGeneratorSimulator,
    ]:
        LOGGER.info(f"\nRunning {sim_class.__name__} ...")
        sim = sim_class()
        sim.run(times)


if __name__ == "__main__":
    main()
