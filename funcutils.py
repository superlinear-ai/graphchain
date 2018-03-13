"""
Utility functions employed by the graphchain module.
"""
import os
import re
import pickle
import inspect
from joblib import hash as joblib_hash


def load_hashchain(path):
    """
    Function that loads a 'hash chain'.
    """
    filename = "hashchain.bin" # hardcoded

    if not os.path.isdir(path):
        os.mkdir(path)
    filepath = os.path.join(path, filename)
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as fid:
            obj = pickle.load(fid)
    else:
        print("Creating a new hash-chain file {}".format(filepath))
        obj = dict()
        write_hashchain(obj, filepath)

    return obj, filepath


def write_hashchain(obj, filepath):
    """
    Function that writes a 'hash chain'.
    """
    with open(filepath, 'wb') as fid:
        pickle.dump(obj, fid)


def strip_decorators(code):
    """
    Function that strips decorator-like lines from a text (code) source.
    """
    return re.sub(r"@[\w|\s][^\n]*\n", "", code).lstrip()


def wrap_to_store(obj, path, objhash):
    """
    Function that wraps a callable object in order to execute it
    and store its result.
    """
    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        assert os.path.isdir(path)
        filepath = os.path.join(path, objhash+'.bin')
        if callable(obj):
            ret = obj(*args, **kwargs)
        else:
            ret = obj
        print("* [{}] EXEC + STORE (hash={})".format(obj.__name__,objhash))
        with open(filepath, 'wb') as fid:
            pickle.dump(ret, fid)
        return ret
    return exec_store_wrapper


def wrap_to_load(obj, path, objhash):
    """
    Function that wraps a callable object in order not to execute it
    and rather load its result.
    """
    def loading_wrapper(): # no arguments needed
        """
        Simple load wrapper.
        """
        assert os.path.isdir(path)
        filepath = os.path.join(path, objhash+'.bin')
        assert os.path.isfile(filepath)
        print("* [{}] LOAD (hash={})".format(obj.__name__, objhash), end="")
        with open(filepath, 'rb') as fid:
            ret = pickle.load(fid)
        return ret
    return loading_wrapper



def get_hash(dsk, key, key_to_hash=None):
    """
    Function that returns the hash corresponding to a dask task
    using the hashes of its dependencies, input arguments and
    source code of the function associated to the task.
    """
    ccontext = dsk.get(key, None) # call context
    assert ccontext is not None

    fnhash_list = []
    arghash_list = []
    dwnstrhash_list = []

    for ccit in ccontext:
        if callable(ccit):
            # function
            f_hash = joblib_hash(strip_decorators(inspect.getsource(ccit)))
            fnhash_list.append(f_hash)
        else:
            if type(key_to_hash) is dict and ccit in key_to_hash.keys():
                # we have a dask graph key
                dwnstrhash_list.append(joblib_hash(key_to_hash[ccit]))
            else:
                if type(ccit) in (int, float, str, list, dict, set):
                    # some other argument
                    arghash_list.append(joblib_hash(ccit))
                else:
                    # ideally one should never reach this stage
                    print("Unrecognized argument type. Raise Hell!")

    objhash = joblib_hash("".join((*fnhash_list, *arghash_list, *dwnstrhash_list)))
    subhashes = (joblib_hash("".join(fnhash_list)),
                 joblib_hash("".join(arghash_list)),
                 joblib_hash("".join(dwnstrhash_list)))

    return objhash, subhashes


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
