import os
import pytest
import dask
from context import graphchain
from graphchain import gcoptimize

@pytest.fixture(scope="function")
def generate_graph():
    """
    Generates a dask graph of the form:
    
		     O top(..)
                 ____|____
		/	  \
   delayed(-3) O           O baz(..)
		  _________|________
                 /                  \
                O boo(...)           O goo(..,v6)
         _______|_______          ___|____
	/       |       \        /        \
       O        O        O      O          O
     foo(.) bar(.)    baz(.)   foo(.)    bar(.)
      |         |        Q      |          |
      |         |        |      |          |
      v1       v2       v3      v4         v5
    """
    # Functions
    def foo(x):
        return x

    def bar(x):
        return x+2

    def baz(*args):
        return sum(args)

    def boo(*args):
        return len(args)+sum(args)

    def goo(*args):
        return sum(args)+1

    def top(x,y):
        return x-y

    from inspect import getsource
    function_code = {}
    for fn in [foo, bar, baz, boo, goo, top]:
        function_code[fn.__name__] = getsource(fn)

    # Graph (for the function definitions above)
    dsk = {"v1":1, "v2":2, "v3":3, "v4":0,
           "v5":-1, "v6":-2, "d1":-3,
           "foo1": (foo, "v1"),
           "foo2": (foo, "v4"),
           "bar1": (bar, "v2"),
           "bar2": (bar, "v5"),
           "baz1": (baz, "v3"),
           "baz2": (baz, "boo1", "goo1"),
           "boo1": (boo, "foo1", "bar1", "baz1"),
           "goo1": (goo, "foo2", "bar2", "v6"),
           "top1": (top, "d1", "baz2")}
    return (dsk, function_code)


@pytest.fixture(scope="module")
def make_tmp_dir():
    dirname = os.path.abspath('__pytest_graphchain_cache__')
    os.mkdir(dirname)
    os.chdir(dirname)
    yield dirname
    os.rmdir(dirname)
    print("Cleanup of {} complete.".format(dirname))


def test_compute(make_tmp_dir, generate_graph):
    dir = make_tmp_dir
    dsk, function_code = generate_graph
    result = dask.get(dsk, ["top1"])
    assert result == (-14,)
