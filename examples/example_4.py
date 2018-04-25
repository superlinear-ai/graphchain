import graphchain
import logging

logging.getLogger().setLevel(logging.DEBUG)


def foo(x):
    return x+1

def bar(*args):
    return sum(args)

dsk = {'foo1':(foo,1), 'foo2':(foo,1), 'top':(bar, 'foo1', 'foo2')}

result = graphchain.get_gcoptimized(dsk, 'top', logfile="stdout")
assert result == 4
