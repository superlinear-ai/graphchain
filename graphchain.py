"""
'Hash chain' optimizer for dask delayed execution graphs.
"""
from collections import deque
from dask.core import get_dependencies
from funcutils import load_hashchain, write_hashchain
from funcutils import wrap_to_load, wrap_to_store, get_hash

DEBUG = 1

def gcoptimize(dsk, keys=None, cachedir="./__graphchain_cache__", hashchain=None):
    """
    Dask graph optimizer. Returns a graph with its taks-associated
    functions modified as to minimize execution times.
    """
    if hashchain is None: # 'hashchain' is a dict f all hashes
        hashchain, filepath = load_hashchain(cachedir)

    key_to_hash = {}                            # key:hash mapping
    key_to_hashmatch = {}                       # key:hash 'matched' mapping
    allkeys = list(dsk.keys())                  # All keys in the graph
    work = deque(allkeys)                       # keys to be traversed
    solved = set()                              # keys of computable tasks
    replacements = dict()                       # what the keys will be replaced with
    dependencies = dict((k, get_dependencies(dsk, k)) for k in allkeys)
    iteration = 0

    while work:
        key = work.popleft()
        deps = dependencies[key]

        if DEBUG:
            print("at it = {}".format(iteration))

        if not deps:
            ### LEAF
            solved.add(key)
            htask, hcomp = get_hash(dsk, key) # get overall taks hash htask and hash components
            key_to_hash[key] = htask

            # Check if the hash matches anything available
            if htask in hashchain.keys():
                # HASH MATCH
                key_to_hashmatch[key] = True
                replacements[key] = (wrap_to_load(cachedir, htask),)
            else:
                # HASH MISMATCH
                key_to_hashmatch[key] = False
                hashchain[htask] = hcomp # update hash-chain entry
                replacements[key] = (wrap_to_store(dsk[key][0], cachedir, htask),
                                     *dsk[key][1:])
            if DEBUG:
                print("key={}, hash={} is a LEAF".format(key, htask))
        else:
            if set(deps).issubset(solved):
                ### SOLVABLE NODE
                solved.add(key)
                htask, hcomp = get_hash(dsk, key, key_to_hash)
                key_to_hash[key] = htask

                if htask in hashchain.keys():
                    # HASH MATCH
                    key_to_hashmatch[key] = True
                    replacements[key] = (wrap_to_load(cachedir, htask),)
                else:
                    # HASH MISMATCH
                    key_to_hashmatch[key] = False
                    hashchain[htask] = hcomp # update hash-chain entry
                    replacements[key] = (wrap_to_store(dsk[key][0], cachedir, htask),
                                         *dsk[key][1:])
                if DEBUG:
                    print("key {}, hash={} is SOLVABLE".format(key, htask))
            else:
                ### Some dependencies are not solvable (yet)
                work.append(key)
                if DEBUG:
                    print("key {} is for LATER.".format(key))

        iteration += 1

    # Write the hashchain
    write_hashchain(hashchain, filepath)

    # Put in the graph the newly wrapped functions
    for key in replacements:
        dsk[key] = replacements[key]

    if DEBUG:
        print("DONE.")


    # Second traversion of the graph for pruning
    # Look at all the nodes:
    # - if a node was not executed: SKIP from analysis
    # - if a node was executed:
    #   - replace all non-executed deps by load operations: i.e. key gets a lambda:load(file) and
    #   the id goes into a pruned list
    #   - leave executed deps intact
    # Traverse again and:
    #  - for all the keys in the pruned list (they should have ONLY un-executed deps)
    #   - remove their (unexecuted) deps from the list

    ### BREAKPOINT ##################
    #if DEBUG:
    #    set_trace()
    #################################

    return dsk
