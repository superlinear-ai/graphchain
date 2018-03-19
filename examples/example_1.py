import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../graphchain')))

import dask
import graphchain
from time import sleep
from graphchain import gcoptimize

def delayed_graph_ex1():

    @dask.delayed
    def foo(x):
        return x+1
    
    @dask.delayed
    def bar(x):
        return x-1

    @dask.delayed
    def baz(*args):
        return sum(args)+1

    @dask.delayed
    def printme(x):
        print(x)
        return 0

    v1 = foo(1) # return 2
    v2 = bar(2) # returns 1
    p1 = printme(".")
    v3 = baz(v1, v2, p1) # returns 3
    v4 = baz(v3, v1,1) # return 5
    v5 = baz(v1,v2,v3,v4) # returns 11
    return (v5, 12) # DAG and expected result 


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
        return len(args)+sum(args)

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
    return  (top1,-14) # DAG and expected result 


def compute_with_graphchain(dsk):
    cachedir = "./__graphchain_cache__"

    with dask.set_options(delayed_optimize = gcoptimize):
        result = dsk.compute(cachedir=cachedir,
                             verbose=True,
                             compression=True)
    return result


def test_ex1():
    dsk, result = delayed_graph_ex1()
    assert compute_with_graphchain(dsk) == result

def test_ex2():
    dsk, result = delayed_graph_ex2()
    assert compute_with_graphchain(dsk) == result

if __name__ == "__main__":
    # test_ex1()
    test_ex2()
