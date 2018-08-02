import time

import dask
import graphchain


def add(x, y):
    return x + y


def sleep(*args):
    time.sleep(1)
    return args


def log(d):
    print(d)
    return d


dsk = {
    'a': (add, 1, 2),
    'b': (add, 1, 2),
    'c': (sleep, 'a'),
    'd': (sleep, 'b'),
    'x': (log, {'C': 'c', 'D': 'd'})
}

# First run example
result = graphchain.get(dsk, ['x'], scheduler=dask.get)

# Second run example
with dask.config.set(scheduler=dask.threaded.get):
    result = graphchain.get(dsk, ['x'])
