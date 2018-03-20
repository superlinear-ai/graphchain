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
    >>> from graphchain import gcoptimize
    >>> with dask.set_options(delayed_optimize = gcoptimize):
            result = dsk.compute(...) # <-- arguments go there

    A full example can be found in `examples/example_1.py`. For more
    documentation on customizing the optimization of dask graphs,
    check the `Customizing Optimization` section from the dask
    documentation at https://dask.pydata.org/en/latest/optimize.html.
"""
from collections import deque, Iterable
from dask.core import get_dependencies
from funcutils import load_hashchain, write_hashchain
from funcutils import wrap_to_load, wrap_to_store, get_hash


def gcoptimize(dsk,
               keys=None,
               cachedir="./__graphchain_cache__",
               no_cache_keys=None,
               verbose=False,
               compression=False):
    """
    Optimizes a dask delayed execution graph by caching individual
    task outputs and by loading the outputs of or executing the minimum
    number of tasks necessary to obtain the requested output.

    Args:
        dsk (dict): Input dask graph.
        keys (list, optional): The dask graph output keys. Defaults to None.
        cachedir (str, optional): The graphchain cache directory.
            Defaults to "./__graphchain_cache__".
        no_cache_keys (list, optional): Keys for which no caching will occur;
            the keys still still contribute to the hashchain.
            Defaults to None.
        verbose (bool, optional): Verbosity option. Defaults to False.
        compression (bool, optional): Enables LZ4 compression of the
            task outputs and hash-chain. Defaults to False.

    Returns:
        dict: An optimized dask graph.
    """
    if keys is None:
        print("'keys' argument is None. Will not optimize input graph.")
        return dsk

    hashchain, filepath = load_hashchain(cachedir, compression=compression)
    allkeys = list(dsk.keys())                  # All keys in the graph
    work = deque(dsk.keys())                    # keys to be traversed
    solved = set()                              # keys of computable tasks
    replacements = dict()                       # what the keys will be replaced with
    dependencies = {k:get_dependencies(dsk, k) for k in allkeys}
    keyhashmaps = {}                            # key:hash mapping

    while work:
        key = work.popleft()
        deps = dependencies[key]

        if not deps or set(deps).issubset(solved):
            ### Leaf or solvable node
            solved.add(key)
            task = dsk.get(key)
            htask, hcomp = get_hash(task, keyhashmaps) # get hashes
            keyhashmaps[key] = htask
            skipcache = key in no_cache_keys

            # Account for different task types: i.e. functions/constants
            if isinstance(task, Iterable):
                fno = task[0]
                fnargs = task[1:]
            else:
                fno = task
                fnargs = []

            # Check if the hash matches anything available
            if htask in hashchain.keys() and not skipcache:
                # Hash match and output cacheable
                fnw = wrap_to_load(fno, cachedir, htask,
                                   verbose=verbose, compression=compression)
                replacements[key] = (fnw,)
            elif htask in hashchain.keys() and skipcache:
                # Hash match and output *non-cachable*
                fnw = wrap_to_store(fno, cachedir, htask,
                                    verbose=verbose,
                                    compression=compression,
                                    skipcache=skipcache)
                replacements[key] = (fnw, *fnargs)
            else:
                # Hash mismatch
                hashchain[htask] = hcomp
                fnw = wrap_to_store(fno, cachedir, htask,
                                    verbose=verbose,
                                    compression=compression,
                                    skipcache=skipcache)
                replacements[key] = (fnw, *fnargs)
        else:
            ### Non-solvable node
            work.append(key)

    # Write the hashchain
    write_hashchain(hashchain, filepath, compression=compression)

    # Put in the graph the newly wrapped functions
    newdsk = dsk.copy()
    for key in replacements:
        newdsk[key] = replacements[key]

    return newdsk
