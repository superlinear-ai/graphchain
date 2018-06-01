"""
Graphchain is a `hash-chain` optimizer for dask delayed execution graphs.
It employes a hashing mechanism to check wether the state associated to
a task that is to be run (i.e. its function source code, input arguments
and other input dask-related dependencies) has already been `hashed`
(and hence, an output is available) or, it is new/changed. Depending
on the current state, the current task becomes a load-from-disk operation
or an execute-and-store-to-disk one. This is done is such a manner that
the minimimum number of load/execute operations are performed, minimizing
both persistency and computational demands.

Examples:
    Applying the hash-chain based optimizer on `dask.delayed` generated
    execution graphs is straightforward, by using the
    >>> from graphchain import optimize
    >>> with dask.set_options(delayed_optimize = optimize):
            result = dsk.compute(...) # <-- arguments go there

    A full example can be found in `examples/example_1.py`. For more
    documentation on customizing the optimization of dask graphs,
    check the `Customizing Optimization` section from the dask
    documentation at https://dask.pydata.org/en/latest/optimize.html.
"""
import warnings
from collections import deque
import dask
from dask.core import get_dependencies, toposort

from .funcutils import (analyze_hash_miss, get_hash, get_storage,
                        load_hashchain, wrap_to_load, wrap_to_store,
                        write_hashchain, get_bottom_tasks)


def optimize(dsk,
             keys=None,
             no_cache_keys=None,
             compress=False,
             cachedir="./__graphchain_cache__"):
    """
    Optimizes a dask delayed execution graph by caching individual
    task outputs and by loading the outputs of or executing the minimum
    number of tasks necessary to obtain the requested output.

    Args:
        dsk (dict): Input dask graph.
        keys (list, optional): The dask graph output keys. Defaults to None.
        no_cache_keys (list, optional): Keys for which no caching will occur;
            the keys still still contribute to the hashchain.
            Defaults to None.
        compress (bool, optional): Enables LZ4 compression of the task outputs.
            Defaults to False.
        cachedir (str, optional): The graphchain cache directory.
            Defaults to "./__graphchain_cache__". You can also specify a
            PyFilesystem FS URL such as "s3://mybucket/__graphchain_cache__" to
            store the cache on another filesystem such as S3.

    Returns:
        dict: An optimized dask graph.
    """
    if keys is None:
        warnings.warn("Nothing to optimize because `keys` argument is `None`.")
        return dsk

    if no_cache_keys is None:
        no_cache_keys = []

    storage = get_storage(cachedir)
    hashchain = load_hashchain(storage, compress=compress)

    allkeys = toposort(dsk)  # All keys in the graph, topologically sorted.
    work = deque(dsk.keys())  # keys to be traversed
    solved = set()  # keys of computable tasks
    dependencies = {k: get_dependencies(dsk, k) for k in allkeys}
    keyhashmaps = {}  # key:hash mapping
    newdsk = dsk.copy()  # output task graph
    hashes_to_store = set()  # list of hashes that correspond # noqa
                             # to keys whose output will be stored # noqa
    while work:
        key = work.popleft()
        deps = dependencies[key]

        if not deps or set(deps).issubset(solved):
            # Leaf or solvable node
            solved.add(key)
            task = dsk.get(key)
            htask, hcomp = get_hash(task, keyhashmaps)
            keyhashmaps[key] = htask
            skipcache = key in no_cache_keys

            # Account for different task types: i.e. functions/constants
            if isinstance(task, tuple):
                # function call node
                fno = task[0]
                fnargs = task[1:]
            elif isinstance(task, str) or isinstance(task, list) or \
                    isinstance(task, dict):
                # graph key node
                def identity(x): return x
                fno = identity
                fnargs = [get_bottom_tasks(dsk, task)]
            else:
                # constant value
                fno = task
                fnargs = []

            # Check if the hash matches anything available
            if (htask in hashchain.keys() and not skipcache
                    and htask not in hashes_to_store):
                # Hash match and output cacheable
                fnw = wrap_to_load(key, fno, storage, htask, compress=compress)
                newdsk[key] = (fnw, )
            elif htask in hashchain.keys() and skipcache:
                # Hash match and output *non-cachable*
                fnw = wrap_to_store(
                    key,
                    fno,
                    storage,
                    htask,
                    compress=compress,
                    skipcache=skipcache)
                newdsk[key] = (fnw, *fnargs)
            else:
                # Hash miss
                analyze_hash_miss(hashchain, htask, hcomp, key, skipcache)
                hashchain[htask] = hcomp
                hashes_to_store.add(htask)
                fnw = wrap_to_store(
                    key,
                    fno,
                    storage,
                    htask,
                    compress=compress,
                    skipcache=skipcache)
                newdsk[key] = (fnw, *fnargs)
        else:
            # Non-solvable node
            work.append(key)

    # Write the hashchain
    write_hashchain(hashchain, storage, compress=compress)
    return newdsk


def get(dsk, keys=None, scheduler=None, **kwargs):
    """A cache-optimized equivalent to dask.get.

    Optimizes an input graph using a hash-chain based approach. I.e., apply
    `graphchain.optimize` to the graph and get the requested keys.

    Args:
        dsk (dict): Input dask graph.
        keys (list, optional): The dask graph output keys. Defaults to None.
        scheduler (optional): dask get method to be used.
        **kwargs (optional) Keyword arguments for the 'optimize' function;
            can be any of the following: 'no_cache_keys', 'compress',
            'cachedir'.

    Returns:
        The computed values corresponding to the desired keys, specified
        in the 'keys' argument.
    """
    newdsk = optimize(dsk, keys, **kwargs)
    scheduler = scheduler or dask.context._globals.get("get") or dask.get
    ret = scheduler(newdsk, keys)
    return ret
