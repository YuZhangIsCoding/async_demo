from async_demo.simulator.Simulator import Simulator
from typing import Iterable, Union
import threading
from async_demo.util import getLogger, time_it
import attr
import time
from concurrent.futures import ThreadPoolExecutor


LOGGER = getLogger(__name__)


class NaiveThreadingSimulator(Simulator):
    """Using the threading module to simulator the multi-threading process for IO-bound problems"""

    @time_it
    def run(self, it: Iterable[Union[int, float]]) -> int:
        """multi-threading run"""
        threads = [
            threading.Thread(target=self.simulate_IO, args=(i, t))
            for i, t in enumerate(it)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        LOGGER.debug(f"Final count for `{self.__class__.__name__}`: {self.count}")

        return self.count


@attr.s(auto_attribs=True)
class ThreadingUnsafeSimulator(NaiveThreadingSimulator):
    """This is to illustrate multi-threading could raise race conditions"""

    def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """add a lag to simulate the race conditions"""
        LOGGER.info(f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ...")
        time.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")

        # this is to illustrate the race condition, if a variable is not locked, the cpu
        # can do preemptive swapping of threads and thus lead to unexpected results
        LOGGER.debug(f"Updating value for task {task_no}")
        tmp = self.count + 1
        time.sleep(self.scale)
        self.count = tmp
        LOGGER.debug(f"Count updated for task {task_no} as {self.count}")


@attr.s(auto_attribs=True)
class ThreadingLockSimulator(NaiveThreadingSimulator):
    """Use lock to prevent race conditions"""

    lock = threading.Lock()

    def simulate_IO(self, task_no: int, lag: Union[int, float], *args, **kwargs):
        """Simulate IO-bound work"""
        LOGGER.info(f"Start simulating task {task_no} for {self.scale * lag:.3f} second(s) ...")
        time.sleep(self.scale * lag)
        LOGGER.info(f"Complete simulation of task {task_no}")

        LOGGER.debug(f"Update the count for task {task_no}")
        with self.lock:
            # within this lock, variables are locked by a thread, which means other threads cannot access
            LOGGER.debug(f"Thread is locked for task {task_no}")
            tmp = self.count + 1
            time.sleep(self.scale)
            self.count = tmp
        LOGGER.debug(f"Thread is released for task {task_no}")
        LOGGER.debug(f"Value updated for task {task_no}")


@attr.s(auto_attribs=True)
class ThreadingPoolSimulator(ThreadingLockSimulator):
    """Use the ThreadPoolExecutor instead of creating new threads every time"""

    max_workers: int = 2

    @time_it
    def run(self, it: Iterable[Union[int, float]]) -> int:
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i, t in enumerate(it):
                executor.submit(self.simulate_IO, task_no=i, lag=t)

        return self.count
