import os
import re
import pickle
import hashlib
import inspect
from joblib import Memory


def load_hashchain(path):
    CHAINFILE = "hashchain.bin" # hardcoded
    
    if not os.path.isdir(path):
        os.mkdir(path)
    filepath = os.path.join(path, CHAINFILE)
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as fid:
            obj = pickle.load(fid)
    else:
        print("Creating a new hash-chain file {}".format(filepath))
        obj = dict()
        write_hashchain(obj, filepath)
    
    return obj, filepath


def write_hashchain(obj, filepath):
    with open(filepath, 'wb') as fid:
        pickle.dump(obj, fid)


def strip_decorators(code):
    return re.sub(r"@[\w|\s][^\n]*\n", "", code).lstrip()


def wrap_to_store(obj, path, hash):
    def exec_wrapper(*args, **kwargs):
        assert os.path.isdir(path)
        filepath = os.path.join(path, hash+'.bin')
        if callable(obj):
            ret = obj(*args, **kwargs) 
        else:
            ret = obj
        print("hash={}, STORING value={}...".format(hash, ret))
        with open(filepath, 'wb') as fid:
            pickle.dump(ret, fid)
        return ret
    return exec_wrapper


def wrap_to_load(path, hash):
    def exec_wrapper(*args, **kwargs):
        assert os.path.isdir(path)
        filepath = os.path.join(path, hash+'.bin')
        assert os.path.isfile(filepath)
        print("hash={}, LOADING...".format(hash), end="")
        with open(filepath, 'rb') as fid:
            ret = pickle.load(fid)
        print(" value={} OK.".format(ret))
        return ret
    return exec_wrapper



from joblib import hash as jl_hash
from collections import namedtuple
def get_hash(dsk, key, key_to_hash=None):
   
    ccontext = dsk.get(key, None) # call context 
    assert ccontext is not None 
    
    # Calculate hashes for all elements contained in the value
    function_hashes = []
    args_hashes = []
    downstream_hashes = []
    for c in ccontext:
        if callable(c):
            # function
            f_hash = jl_hash(strip_decorators(inspect.getsource(c)))
            function_hashes.append(f_hash)
        else:
            if type(key_to_hash) is dict and c in key_to_hash.keys():
                # we have a dask graph key
                downstream_hashes.append(jl_hash(key_to_hash[c]))
            else:
                if type(c) in (int, float, str, list, dict, set):
                    # some other argument
                    args_hashes.append(jl_hash(c))
                else:
                    # ideally one should never reach this stage
                    print("Unrecognized argument type. Raise Hell!")

    
    # Return hashchain entry of the form
    # (hash(everything), (hash(function), hash(arguments), hash(downstream))

    # TODO: REDO the whole hash calculation mechanism
    return (jl_hash("".join((*function_hashes, *args_hashes, *downstream_hashes))),
            (jl_hash("".join(function_hashes)), # hashes of the code, arguments and downseteam stuff
             jl_hash("".join(args_hashes)),
             jl_hash("".join(downstream_hashes)))
            )


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
