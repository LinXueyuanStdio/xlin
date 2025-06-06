from timeit import default_timer as timer
from functools import wraps
import time


class Benchmark(object):

    def __init__(self, msg, fmt="%0.3g"):
        self.msg = msg
        self.fmt = fmt

    def __enter__(self):
        self.start = timer()
        return self

    def __exit__(self, *args):
        t = timer() - self.start
        print(("%s : " + self.fmt + " seconds") % (self.msg, t))
        self.time = t


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print(f'func:{f.__name__!r} args:[{args!r}, {kw!r}] took: {te - ts:2.4f} sec')
        return result

    return wrap


class Timer:
    """ Simple block which can be called as a context, to know the time of a block. """

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

if __name__ == "__main__":
    with Timer() as t:
        time.sleep(1)
    print(t.interval)
    with Benchmark("Test Benchmark") as b:
        time.sleep(1)
    print(b.time)
    @timing
    def test_function(x, y):
        time.sleep(1)
        return x + y
    result = test_function(1, 2)
    print(f"Result of test_function: {result}")