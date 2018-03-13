"""
'Hash chain' optimizer for dask delayed execution graphs.
"""
from collections import deque
from dask.core import get_dependencies
from funcutils import load_hashchain, write_hashchain
from funcutils import wrap_to_load, wrap_to_store, get_hash


def gcoptimize(dsk, keys=None, cachedir="./__graphchain_cache__", hashchain=None):
    """
    Dask graph optimizer. Returns a graph with its taks-associated
    functions modified as to minimize execution times.
    """
    if hashchain is None: # 'hashchain' is a dict f all hashes
        hashchain, filepath = load_hashchain(cachedir)

    keyhashmaps = {}                            # key:hash mapping
    hashmatch = {}                              # key:hash 'matched' mapping
    allkeys = list(dsk.keys())                  # All keys in the graph
    work = deque(allkeys)                       # keys to be traversed
    solved = set()                              # keys of computable tasks
    replacements = dict()                       # what the keys will be replaced with
    dependencies = dict((k, get_dependencies(dsk, k)) for k in allkeys)

    while work:
        key = work.popleft()
        deps = dependencies[key]

        if not deps or set(deps).issubset(solved):
            ### LEAF or SOLVABLE NODE
            solved.add(key)
            htask, hcomp = get_hash(dsk, key, keyhashmaps) # get overall taks hash htask and hash components
            keyhashmaps[key] = htask

            # Check if the hash matches anything available
            if htask in hashchain.keys():
                # HASH MATCH
                hashmatch[key] = True
                replacements[key] = (wrap_to_load(dsk[key][0], cachedir, htask),)
            else:
                # HASH MISMATCH
                hashmatch[key] = False
                hashchain[htask] = hcomp # update hash-chain entry
                replacements[key] = (wrap_to_store(dsk[key][0], cachedir, htask),
                                     *dsk[key][1:])
        else:
            ### NON-SOLVABLE NODE  
            work.append(key)

    # Write the hashchain
    write_hashchain(hashchain, filepath)

    # Put in the graph the newly wrapped functions
    for key in replacements:
        dsk[key] = replacements[key]

    return dsk
