import inspect
import logging
import os
import time
from functools import wraps
from typing import Callable, Optional

LOGLEVEL = os.environ.get("LOGLEVEL", logging.INFO)
logging.basicConfig(
    level=LOGLEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def getLogger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name)


def time_it(func: Callable):
    """Decorator to log the time spent"""
    logger = getLogger(inspect.getmodule(func).__name__)

    @wraps(func)
    def wrapper(*args, **kwargs):
        ts = time.time()
        res = func(*args, **kwargs)
        te = time.time()
        logger.info(f"Finish {func.__name__} in {te-ts: .3f} second(s)")
        return res

    return wrapper
