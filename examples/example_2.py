from time import sleep

import dask
from graphchain import optimize


def delayed_graph_example():

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
    # hash miss
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

    skipkeys = [boo1.key]
    return (top1, -14, skipkeys)  # DAG and expected result


def compute_with_graphchain(dsk, skipkeys):
    with dask.config.set(delayed_optimize=optimize):
        result = dsk.compute(
            cachedir="./__graphchain_cache__", no_cache_keys=skipkeys)
    return result


def test_example():
    dsk, result, skipkeys = delayed_graph_example()
    assert compute_with_graphchain(dsk, skipkeys) == result


if __name__ == "__main__":
    test_example()
