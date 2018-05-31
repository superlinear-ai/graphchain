import logging
from time import sleep

import dask

from graphchain import optimize

logging.getLogger("graphchain.graphchain").setLevel(logging.DEBUG)
logging.getLogger("graphchain.funcutils").setLevel(logging.INFO)


def delayed_graph_ex1():
    @dask.delayed
    def foo(x):
        return x + 1

    @dask.delayed
    def bar(*args):
        return sum(args) + 1

    v1 = foo(1)  # return 2
    v2 = foo(1)  # returns 2
    v3 = bar(v1, v2, 1)  # returns 6
    return (v3, 6)  # DAG and expected result


def delayed_graph_ex2():

    # Functions
    @dask.delayed
    def foo(argument):
        sleep(1)
        return argument

    @dask.delayed
    def bar(argument):
        sleep(1)
        return argument + 2

    @dask.delayed
    def baz(*args):
        sleep(1)
        return sum(args)

    @dask.delayed
    def boo(*args):
        sleep(1)
        return len(args) + sum(args)

    @dask.delayed
    def goo(*args):
        sleep(1)
        return sum(args) + 1

    @dask.delayed
    def top(argument, argument2):
        sleep(3)
        return argument - argument2

    # Constants
    v1 = dask.delayed(1)
    v2 = dask.delayed(2)
    v3 = dask.delayed(3)
    v4 = dask.delayed(0)
    v5 = dask.delayed(-1)
    v6 = dask.delayed(-2)
    d1 = dask.delayed(-3)

    boo1 = boo(foo(v1), bar(v2), baz(v3))
    goo1 = goo(foo(v4), v6, bar(v5))
    baz2 = baz(boo1, goo1)
    top1 = top(d1, baz2)
    return (top1, -14)  # DAG and expected result


def compute_with_graphchain(dsk):
    cachedir = "./__graphchain_cache__"

    with dask.set_options(delayed_optimize=optimize):
        result = dsk.compute(cachedir=cachedir, compression=True)
    return result


def test_ex1():
    dsk, result = delayed_graph_ex1()
    assert compute_with_graphchain(dsk) == result


def test_ex2():
    dsk, result = delayed_graph_ex2()
    assert compute_with_graphchain(dsk) == result


if __name__ == "__main__":
    test_ex1()
    test_ex2()
