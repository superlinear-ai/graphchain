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
from funcutils import (init_logging,
                       get_storage,
                       get_hash,
                       load_hashchain, write_hashchain,
                       wrap_to_load, wrap_to_store,
                       analyze_hash_miss)


def gcoptimize(dsk,
               keys=None,
               no_cache_keys=None,
               logfile=None,
               compression=False,
               cachedir="./__graphchain_cache__",
               persistency="local",
               s3bucket=""):
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
        logfile (str, optional): A file to be used for logging.
            Possible values are None (do not log anything),
            "stdout" (print to STDOUT) or "<any string>" which will
            create a log file with the argument's name.
            Defaults to None.
        compression (bool, optional): Enables LZ4 compression of the
            task outputs. Defaults to False.
        cachedir (str, optional): The graphchain cache directory.
            Defaults to "./__graphchain_cache__".
        persistency (str, optional): Specifies the type of disk persistency
            to be used for the `hash-chain` and cache files. Two options
            are supported: "local" for local disk storage and "s3" for
            Amazon S3 storage. Defaults to "local".
        s3bucket (str, optional): The name of the Amazon S3 bucket where
            the file cache is stored. Defaults to "".
    Returns:
        dict: An optimized dask graph.
    """
    if keys is None:
        print("[WARNING] 'keys' argument is None. Will not optimize.")
        return dsk

    if no_cache_keys is None:
        no_cache_keys = []

    init_logging(logfile)  # initializes logging
    storage = get_storage(cachedir, persistency, s3bucket=s3bucket)
    hashchain = load_hashchain(storage, compression=compression)

    allkeys = list(dsk.keys())                  # All keys in the graph
    work = deque(dsk.keys())                    # keys to be traversed
    solved = set()                              # keys of computable tasks
    replacements = dict()                       # replacements of graph tasks
    dependencies = {k: get_dependencies(dsk, k) for k in allkeys}
    keyhashmaps = {}                            # key:hash mapping

    while work:
        key = work.popleft()
        deps = dependencies[key]

        if not deps or set(deps).issubset(solved):
            ### Leaf or solvable node
            solved.add(key)
            task = dsk.get(key)
            htask, hcomp = get_hash(task, keyhashmaps)
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
                fnw = wrap_to_load(fno, storage, htask,
                                   compression=compression)
                replacements[key] = (fnw,)
            elif htask in hashchain.keys() and skipcache:
                # Hash match and output *non-cachable*
                fnw = wrap_to_store(fno, storage, htask,
                                    compression=compression,
                                    skipcache=skipcache)
                replacements[key] = (fnw, *fnargs)
            else:
                # Hash miss
                analyze_hash_miss(hashchain, htask, hcomp, key)
                hashchain[htask] = hcomp
                fnw = wrap_to_store(fno, storage, htask,
                                    compression=compression,
                                    skipcache=skipcache)
                replacements[key] = (fnw, *fnargs)
        else:
            ### Non-solvable node
            work.append(key)

    # Write the hashchain
    write_hashchain(hashchain, storage, compression=compression)

    # Put in the graph the newly wrapped functions
    newdsk = dsk.copy()
    for key in replacements:
        newdsk[key] = replacements[key]

    return newdsk
