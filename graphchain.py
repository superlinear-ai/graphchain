"""
'Hash chain' optimizer for dask delayed execution graphs.
"""
from collections import deque
from dask.core import get_dependencies
from funcutils import load_hashchain, write_hashchain
from funcutils import wrap_to_load, wrap_to_store, get_hash
from funcutils import isiterable


def gcoptimize(dsk,
               keys=None,
               cachedir="./__graphchain_cache__",
               hashchain=None,
               verbose=False):
    """
    Dask graph optimizer. Returns a graph with its taks-associated
    functions modified as to minimize execution times.
    """
    if keys is None:
        print("'keys' argument is None. Will not optimize input graph.")
        return dsk

    if hashchain is None: # 'hashchain' is a dict f all hashes
        hashchain, filepath = load_hashchain(cachedir)

    allkeys = list(dsk.keys())                  # All keys in the graph
    work = deque(allkeys)                       # keys to be traversed
    solved = set()                              # keys of computable tasks
    replacements = dict()                       # what the keys will be replaced with
    dependencies = dict((k, get_dependencies(dsk, k)) for k in allkeys)
    keyhashmaps = {}                            # key:hash mapping
    keyhashmatch = {}                           # key:hash 'matched' mapping

    while work:
        key = work.popleft()
        deps = dependencies[key]

        if not deps or set(deps).issubset(solved):
            ### LEAF or SOLVABLE NODE
            solved.add(key)
            task = dsk.get(key, None)
            htask, hcomp = get_hash(task, keyhashmaps) # get hashes
            keyhashmaps[key] = htask

            # Account for different task types: i.e. functions/constants
            if isiterable(task):
                fno = task[0]
                fnargs = task[1:]
            else:
                fno = task
                fnargs = []

            # Check if the hash matches anything available
            if htask in hashchain.keys():
                # HASH MATCH
                keyhashmatch[key] = True
                fnw = wrap_to_load(fno, cachedir, htask, verbose=verbose)
                replacements[key] = (fnw,)
            else:
                # HASH MISMATCH
                keyhashmatch[key] = False
                hashchain[htask] = hcomp # update hash-chain entry
                fnw = wrap_to_store(fno, cachedir, htask, verbose=verbose)
                replacements[key] = (fnw, *fnargs)
        else:
            ### NON-SOLVABLE NODE
            work.append(key)

    # Write the hashchain
    write_hashchain(hashchain, filepath)

    # Put in the graph the newly wrapped functions
    for key in replacements:
        dsk[key] = replacements[key]

    return dsk
