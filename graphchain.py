#import os
#import re
#import pickle
#import hashlib
#import dask
#import inspect

from dask.core import get_dependencies
#def isleaf(vals, dsk_keys):
#    """
#    Function that checks that the input is a valid value for a dask graph leaf.
#    """
#    checkout = True
#    if type(vals) == int or type(vals) == float or type(vals) == bool:
#        checkout &= True
#    elif type(vals) == str and vals not in dsk_keys:
#        checkout &= True
#    elif type(vals) == list or type(vals) == set or type(vals) == tuple:
#        checkout &= all(isleaf(v, dsk_keys) for v in vals)
#    elif type(vals) == dict:
#        checkout &= all(isleaf(v, dsk_keys) for v in vals.values())
#        checkout &= all(isleaf(k, dsk_keys) for k in vals.keys())
#    elif callable(vals):
#        checkout &= True
#    else:
#        return False
#
#    return checkout


DEBUG = 1
#TODO: Use keys as well (meaning, prune only dependencies of the specified keys)
def gcoptimize(dsk, keys, cachedir="./__graphchain_cache__", memobject=None):

    ### DEBUG ################ 
    if DEBUG: 
        from pdb import set_trace
        set_trace()
    ##########################

    allkeys = set(dsk.keys())
    work = allkeys
    solved = set()
    dependencies = dict((k, get_dependencies(dsk, k)) for k in allkeys)

    # Traverse graph and verify the full chain
    # execute whatever would be necessary
    iteration = 0
    while work:
        key = work.pop()
        deps = dependencies[key]
        if DEBUG: print("at it = {}".format(iteration))
        if not deps:
            ### Leaf
            solved.add(key)
            #   - get function arguments, code
            #   - if the calculate(hash) exists in the hashlist
            #       - add id:hash entry   
            #       - set execute = False
            #     else
            #       - execute
            #       - re-calculate hash
            #       - add id:hash entry
            #       - add hash to hashlist
            #       - set execute=True
            #   - update id:hash
            print("key {} is a LEAF".format(key))
        else:
            if set(deps).issubset(solved):
                ### All dependencies are solved
                solved.add(key)
                #   - get function arguments, code
                #   - if the calculate(hash) exists in the hashlist
                #       - add id:hash entry   
                #       - set execute = False
                #     else
                #       - execute (by loading all deps results)
                #       - re-calculate hash
                #       - add hash to hashlist
                #       - add id:hash entry
                #       - set execute=True
                if DEBUG: print("key {} is SOLVABLE".format(key))
            else:
                ### Some dependencies are not solvable (yet)
                to_solve = set(deps) - solved
                work.add(key)

                if DEBUG: print("key {} is for LATER.".format(key, to_solve))
        iteration += 1

    if DEBUG: print("DONE.")


    # TODO: Maybe make a prune(dsk, nodes) method where nodes fill the condition that they were not
    # executed HOWEVER, their upstream has.


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

    return dsk



