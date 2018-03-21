"""
Utility functions employed by the graphchain module.
"""
import os
import pickle
from collections import Iterable
import lz4.frame
from joblib import hash as joblib_hash
from joblib.func_inspect import get_func_code as joblib_getsource


def load_hashchain(path, compression=False):
    """
    Loads the `hash-chain` found in directory ``path``.
    """
    if compression:
        filename = "hashchain.pickle.lz4"
    else:
        filename = "hashchain.pickle"

    if not os.path.isdir(path):
        os.mkdir(path)
    filepath = os.path.join(path, filename)
    if os.path.isfile(filepath):
        if compression:
            with lz4.frame.open(filepath, mode="r") as fid:
                data = fid.read()
            obj = pickle.loads(data)
        else:
            with open(filepath, "rb") as fid:
                obj = pickle.load(fid)
    else:
        print(f"Creating a new hash-chain file {filepath}")
        obj = dict()
        write_hashchain(obj, filepath, compression=compression)

    return obj, filepath


def write_hashchain(obj, filepath, compression=False):
    """
    Writes a `hash-chain` contained in ``obj`` to a file
    indicated by ``filepath``.
    """
    if compression:
        with lz4.frame.open(filepath, mode="wb") as fid:
            fid.write(pickle.dumps(obj))
    else:
        with open(filepath, "wb") as fid:
            pickle.dump(obj, fid)


def wrap_to_store(obj, path, objhash,
                  verbose=False,
                  compression=False,
                  skipcache=False):
    """
    Wraps a callable object in order to execute it and store its result.
    """
    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        assert os.path.isdir(path)

        if callable(obj):
            ret = obj(*args, **kwargs)
            objname = obj.__name__
        else:
            ret = obj
            objname = "constant=" + str(obj)

        if verbose:
            if compression and not skipcache:
                print(f"* [{objname}] EXEC-STORE-COMPRESS (hash={objhash})")
            elif not compression and not skipcache:
                print(f"* [{objname}] EXEC-STORE (hash={objhash})")
            else:
                print(f"* [{objname}] EXEC *ONLY* (hash={objhash})")

        if not skipcache:
            data = pickle.dumps(ret)
            if compression:
                filepath = os.path.join(path, objhash + ".pickle.lz4")
                data = lz4.frame.compress(data)
            else:
                filepath = os.path.join(path, objhash + ".pickle")

            with open(filepath, "wb") as fid:
                fid.write(data)

        return ret

    return exec_store_wrapper


def wrap_to_load(obj, path, objhash, verbose=False, compression=False):
    """
    Wraps a callable object in order not to execute it and rather
    load its result.
    """
    def loading_wrapper(): # no arguments needed
        """
        Simple load wrapper.
        """
        assert os.path.isdir(path)
        if compression:
            filepath = os.path.join(path, objhash + ".pickle.lz4")
        else:
            filepath = os.path.join(path, objhash + ".pickle")
        assert os.path.isfile(filepath)

        if callable(obj):
            objname = obj.__name__
        else:
            objname = "constant=" + str(obj)

        if verbose:
            if compression:
                print(f"* [{objname}] LOAD-UNCOMPRESS (hash={objhash})")
            else:
                print(f"* [{objname}] LOAD (hash={objhash})")

        if compression:
            with lz4.frame.open(filepath, mode="r") as fid:
                ret = pickle.loads(fid.read())
        else:
            with open(filepath, "rb") as fid:
                ret = pickle.load(fid)
        return ret

    return loading_wrapper


def get_hash(task, keyhashmap=None):
    """
    Calculates and returns the hash corresponding to a dask task
    ``task`` using the hashes of its dependencies, input arguments
    and source code of the function associated to the task. Any
    available hashes are passed in ``keyhashmap``.
    """
    assert task is not None

    fnhash_list = []
    arghash_list = []
    depshash_list = []

    if isinstance(task, Iterable):
        # An iterable (tuple) would correspond to a delayed function
        for taskelem in task:
            if callable(taskelem):
                # function
                sourcecode = joblib_getsource(taskelem)[0]
                fnhash_list.append(joblib_hash(sourcecode))
            else:
                if isinstance(keyhashmap, dict) and taskelem in keyhashmap.keys():
                    # we have a dask graph key
                    depshash_list.append(keyhashmap[taskelem])
                else:
                    arghash_list.append(joblib_hash(taskelem))
    else:
        # A non iterable i.e. constant
        arghash_list.append(joblib_hash(task))

    # Account for the fact that dependencies are also arguments
    arghash_list.append(joblib_hash(joblib_hash(len(depshash_list))))

    subhashes = (joblib_hash("".join(fnhash_list)),
                 joblib_hash("".join(arghash_list)),
                 joblib_hash("".join(depshash_list)))

    objhash = joblib_hash("".join(subhashes))

    return objhash, subhashes
