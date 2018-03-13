import unittest
import dask
import context
from context import graphchain
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
        return sum(args)

    @dask.delayed
    def printme(x):
        print(x)
        return 0

    v1 = foo(1) # return 2
    v2 = bar(2) # returns 1
    p1 = printme(".")
    #dc1 = dask.delayed(100)
    v3 = baz(v1, v2, p1) # returns 3
    v4 = baz(v3, v1) # return 5
    v5 = baz(v1,v2,v3,v4) # returns 11
    return (v5, 11) # DAG and expected result 


def compute_with_graphchain(dsk):
    cachedir = "./__graphchain_cache__" 

    with dask.set_options(delayed_optimize = gcoptimize):
        result = dsk.compute(cachedir = cachedir)
    return result


class TestGraphchain(unittest.TestCase):

    def test_ex1(self):
        dsk, result = delayed_graph_ex1()
        self.assertEqual(compute_with_graphchain(dsk), result)


if __name__ == "__main__":
    unittest.main()
