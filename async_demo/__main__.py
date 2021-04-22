#!/usr/bin/env python
import click

from async_demo.async_generator import run_generator
from async_demo.event_loop_demo import main as event_loop_main
from async_demo.simulator.AsyncSimulator import (
    AsyncBatchSimulator,
    AsyncFutureSimulator,
    AsyncGeneratorSimulator,
    AsyncQueueSimulator,
    AsyncSafeSimulator,
    AsyncSemaphoreSimulator,
    AsyncSimulator,
    AsyncUnsafeSimulator,
)
from async_demo.simulator.MultiprocessingSimulator import (
    MultiprocessingQueueSimulator,
    MultiprocessingSimulator,
    MultiprocessingUnsafeSimulator,
)
from async_demo.simulator.Simulator import Simulator
from async_demo.simulator.ThreadingSimulator import (
    NaiveThreadingSimulator,
    ThreadingLockSimulator,
    ThreadingPoolLazySimulator,
    ThreadingPoolSimulator,
    ThreadingUnsafeSimulator,
)
from async_demo.util import getLogger

from .__init__ import __version__

LOGGER = getLogger(__name__)


@click.group()
@click.version_option(version=__version__)
def main():
    pass


@main.command(help="Show the potential race condition in the multi-threading approach")
@click.option(
    "--n-time", type=click.IntRange(min=1, max=10), default=2, help="Number of times"
)
def show_race(n_time: int):
    times = (1 for _ in range(n_time))

    res = []
    for sim_class in [
        ThreadingUnsafeSimulator,
        ThreadingLockSimulator,
        MultiprocessingUnsafeSimulator,
        MultiprocessingQueueSimulator,
        AsyncUnsafeSimulator,
        AsyncSafeSimulator,
    ]:
        LOGGER.info(f"\nRunning {sim_class.__name__} ...")
        sim = sim_class()
        res.append((sim_class.__name__, sim.run(times)))

    for class_name, count in res:
        LOGGER.info(f"`{class_name}` count: {count}")


@main.command(help="Show all simulators")
@click.option(
    "--times",
    multiple=True,
    type=click.IntRange(min=1, max=10),
    default=[2, 3, 1, 2],
    help="Times to simulate",
)
def show_all(times):
    for sim_class in [
        Simulator,
        NaiveThreadingSimulator,
        ThreadingPoolSimulator,
        ThreadingPoolLazySimulator,
        MultiprocessingSimulator,
        AsyncSimulator,
        AsyncBatchSimulator,
        AsyncQueueSimulator,
        AsyncSemaphoreSimulator,
        AsyncGeneratorSimulator,
        AsyncFutureSimulator,
    ]:
        LOGGER.info(f"\n\nRunning {sim_class.__name__} ...")
        sim = sim_class()
        sim.run(times)


@main.command(help="Demo of event loop")
def event_loop_demo():
    event_loop_main()


@main.command(help="Demo async generator")
@click.option(
    "--positive-int", type=click.IntRange(min=0), default=5, help="Positive number"
)
def async_generator(positive_int: int):
    run_generator(positive_int)


if __name__ == "__main__":
    main()
