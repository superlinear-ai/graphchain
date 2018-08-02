import dask
import graphchain


def foo(x):
    return x + 1


def bar(*args):
    return sum(args)


dsk = {'foo1': (foo, 1), 'foo2': (foo, 1), 'top': (bar, 'foo1', 'foo2')}
keys = ['top']

# First run example
result = graphchain.get(dsk, ['top'], scheduler=dask.get)
assert result == (4, )

# Second run example
with dask.config.set(scheduler=dask.threaded.get):
    result = graphchain.get(dsk, keys)
assert result == (4, )
