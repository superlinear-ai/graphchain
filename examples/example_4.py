import dask
import graphchain


def foo(x):
    return x + 1


def bar(*args):
    return sum(args)


dsk = {'foo1': (foo, 1), 'foo2': (foo, 1), 'top': (bar, 'foo1', 'foo2')}
keys = ['top']

# First run example
dsk = graphchain.optimize(dsk)
result = dask.get(dsk, ['top'], get=dask.get)
assert result == (4, )

# Second run example
with dask.config.set(get=dask.threaded.get):
    result = dask.get(dsk, keys)
assert result == (4, )
