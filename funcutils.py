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


def wrap_to_store(obj, path, objhash, verbose=False):
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
            objname = obj.__name__
        else:
            ret = obj
            objname = 'constant=' + str(obj)

        if verbose:
            print("* [{}] EXEC + STORE (hash={})".format(objname, objhash))

       with open(filepath, 'wb') as fid:
            pickle.dump(ret, fid)
        return ret

    return exec_store_wrapper


def wrap_to_load(obj, path, objhash, verbose=False):
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

        if verbose:
            print("* [{}] LOAD (hash={})".format(obj.__name__, objhash), end="")

        with open(filepath, 'rb') as fid:
            ret = pickle.load(fid)
        return ret

    return loading_wrapper


def isiterable(obj):
    """
    Function that returns True if its argument is iterable
    and False otherwise.
    """
    return (hasattr(obj, "__getitem__") or
            hasattr(obj, "__iter__"))



def get_hash(dsk, key, keyhashmap=None):
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

    if isiterable(ccontext):
        # An iterable (tuple) would correspond to a delayed function
        for ccit in ccontext:
            if callable(ccit):
                # function
                sourcecode = strip_decorators(inspect.getsource(ccit))
                fnhash = joblib_hash(sourcecode)
                fnhash_list.append(fnhash)
            else:
                if type(keyhashmap) is dict and ccit in keyhashmap.keys():
                    # we have a dask graph key
                    dwnstrhash_list.append(joblib_hash(keyhashmap[ccit]))
                else:
                    if type(ccit) in (int, float, str, list, dict, set):
                        # some other argument
                        arghash_list.append(joblib_hash(ccit))
                    else:
                        # ideally one should never reach this stage
                        print("Unrecognized argument type. Raise Hell!")
    else:
        # A non iterable i.e. constant
        arghash_list.append(joblib_hash(ccontext))

    objhash = joblib_hash("".join((*fnhash_list,
                                   *arghash_list,
                                   *dwnstrhash_list)))

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
