from datetime import datetime, timezone
import psutil
import ctypes
from parwop.settings import IS_LINUX, DEBUG


class PerformanceLogger:

    def __init__(self, message=None, ignore_debug: bool = False):
        self.start = None
        self.message = message
        self.ignore_debug = ignore_debug

    def __enter__(self):
        self.start = datetime.now(timezone.utc)
        return self

    def __exit__(self, *args):

        elapsed = datetime.now(timezone.utc) - self.start
        if DEBUG or self.ignore_debug:
            print(f"{self.message}: {elapsed}")

class PerformanceLoggerDecorator:
    def __init__(self, message=None, ignore_debug: bool = False):
        self.message = message
        self.ignore_debug = ignore_debug

    def __call__(self, func):
        message = self.message
        if message is None:
            message = func.__name__

        def wrapper(*args, **kwargs):
            with PerformanceLogger(message=message, ignore_debug=self.ignore_debug):
                return func(*args, **kwargs)
        return wrapper


MEM_SUFFIXES = {
    0: "bytes",
    1: "KiB",
    2: "MiB",
    3: "GiB",
    4: "TiB"
}


def get_rss(humanize: bool = False, print_res: bool = False):
    rss = psutil.Process().memory_info().rss
    if humanize:
        str_repr = humanize_size(rss)
    else:
        str_repr = f"{round(rss, 3)} bytes"

    if print_res:
        print(str_repr)

    return str_repr


def humanize_size(size: int | float) -> str:
    i = 0
    while size >= 1024.:
        size /= 1024.
        i += 1

    return f"{round(size, 3)} {MEM_SUFFIXES[i]}"


if IS_LINUX:

    _LIBC = ctypes.CDLL("libc.so.6")
    print("Using libc malloc_trim")

    def trim_memory():
        return _LIBC.malloc_trim(0)
else:
    print("Unable to use malloc_trim")

    def trim_memory():
        return 0
