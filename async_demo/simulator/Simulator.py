import time
from typing import Iterable, Union

import attr

from async_demo.util import getLogger, time_it

LOGGER = getLogger(__name__)


@attr.s(auto_attribs=True)
class Simulator:
    """This is the basic class for the simulation of processing common IO-bound works"""

    count: int = 0
    scale: float = 0.1
    race: bool = True

    def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        LOGGER.info(
            f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ..."
        )
        time.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")
        self.count += 1

    @time_it
    def run(self, it: Iterable[Union[int, float]]) -> int:
        """Run simulations, and return the final number"""
        for i, t in enumerate(it):
            self.simulate_IO(i, t)
        LOGGER.debug(f"Final count for `{self.__class__.__name__}`: {self.count}")

        return self.count
