import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(__name__.split(".")[0]).version
except Exception:
    __version__ = "unknown"
