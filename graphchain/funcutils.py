"""
Utility functions employed by the graphchain module.
"""
import pickle
import json
import logging
from collections import Iterable
from os import makedirs
from os.path import join, isdir, isfile
import lz4.frame
from joblib import hash as joblib_hash
from joblib.func_inspect import get_func_code as joblib_getsource


class HashchainCompressionMismatch(EnvironmentError):
    """
    Simple exception that is raised whenever the compression
    option in the `gcoptimize` function does not match the one
    present in the `hashchain.json` file if such file exists.
    """
    pass


def load_hashchain(path, compression=False):
    """
    Loads the `hash-chain` found in directory ``path``.
    """
    if not isdir(path):
        makedirs(path, exist_ok=True)
    filepath = join(path, "hashchain.json")

    if not isfile(filepath):
        logging.info(f"Creating a new hash-chain file {filepath}")
        obj = dict()
        write_hashchain(obj, filepath, compression=compression)
        return obj, filepath
    else:
        with open(filepath, "r") as fid:
            hashchaindata = json.loads(fid.read())
        compr_option_lz4 = hashchaindata["compression"] == "lz4"
        obj = hashchaindata["hashchain"]
        if not compr_option_lz4 ^ compression:
            return obj, filepath
        else:
            raise HashchainCompressionMismatch(
                f"Compression option mismatch: "
                f"file={compr_option_lz4}, "
                f"optimizer={compression}.")


def write_hashchain(obj, filepath, version=1, compression=False):
    """
    Writes a `hash-chain` contained in ``obj`` to a file
    indicated by ``filepath``.
    """
    hashchaindata = {"version": str(version),
                     "compression": "lz4" if compression else "none",
                     "hashchain": obj}

    with open(filepath, "w") as fid:
        fid.write(json.dumps(hashchaindata, indent=4))


def wrap_to_store(obj, path, objhash, compression=False, skipcache=False):
    """
    Wraps a callable object in order to execute it and store its result.
    """
    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        assert isdir(path)
        storagepath = join(path, "__cache__")

        if not isdir(storagepath):
            makedirs(storagepath, exist_ok=True)

        if callable(obj):
            ret = obj(*args, **kwargs)
            objname = obj.__name__
        else:
            ret = obj
            objname = "constant=" + str(obj)

        if compression and not skipcache:
            logging.info(f"* [{objname}] EXEC-STORE-COMPRESS (hash={objhash})")
        elif not compression and not skipcache:
            logging.info(f"* [{objname}] EXEC-STORE (hash={objhash})")
        else:
            logging.info(f"* [{objname}] EXEC *ONLY* (hash={objhash})")

        if not skipcache:
            data = pickle.dumps(ret)
            if compression:
                filepath = join(storagepath, objhash + ".pickle.lz4")
                data = lz4.frame.compress(data)
            else:
                filepath = join(storagepath, objhash + ".pickle")

            with open(filepath, "wb") as fid:
                fid.write(data)

        return ret

    return exec_store_wrapper


def wrap_to_load(obj, path, objhash, compression=False):
    """
    Wraps a callable object in order not to execute it and rather
    load its result.
    """
    def loading_wrapper():  # no arguments needed
        """
        Simple load wrapper.
        """
        storagepath = join(path, "__cache__")
        assert isdir(storagepath)

        if compression:
            filepath = join(storagepath, objhash + ".pickle.lz4")
        else:
            filepath = join(storagepath, objhash + ".pickle")
        assert isfile(filepath)

        if callable(obj):
            objname = obj.__name__
        else:
            objname = "constant=" + str(obj)

        if compression:
            logging.info(f"* [{objname}] LOAD-UNCOMPRESS (hash={objhash})")
        else:
            logging.info(f"* [{objname}] LOAD (hash={objhash})")

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
    dephash_list = []

    if isinstance(task, Iterable):
        # An iterable (tuple) would correspond to a delayed function
        for taskelem in task:
            if callable(taskelem):
                # function
                sourcecode = joblib_getsource(taskelem)[0]
                fnhash_list.append(joblib_hash(sourcecode))
            else:
                if (isinstance(keyhashmap, dict) and
                        taskelem in keyhashmap.keys()):
                    # we have a dask graph key
                    dephash_list.append(keyhashmap[taskelem])
                else:
                    arghash_list.append(joblib_hash(taskelem))
    else:
        # A non iterable i.e. constant
        arghash_list.append(joblib_hash(task))

    # Account for the fact that dependencies are also arguments
    arghash_list.append(joblib_hash(joblib_hash(len(dephash_list))))

    # Calculate subhashes
    src_hash = joblib_hash("".join(fnhash_list))
    arg_hash = joblib_hash("".join(arghash_list))
    dep_hash = joblib_hash("".join(dephash_list))

    subhashes = {"src": src_hash, "arg": arg_hash, "dep": dep_hash}
    objhash = joblib_hash(src_hash + arg_hash + dep_hash)
    return objhash, subhashes


def analyze_hash_miss(hashchain, htask, hcomp, taskname):
    """
    Function that analyzes and gives out a printout of
    possible hass miss reasons. The importance of a
    candidate is calculated as Ic = Nm/Nc where:
        - Ic is an imporance coefficient;
        - Nm is the number of subhashes matched;
        - Nc is the number that candidate code
        appears.
    For example, if there are 1 candidates with
    a code 2 (i.e. arguments hash match) and
    10 candidates with code 6 (i.e. code and
    arguments match), the more important candidate
    is the one with a sing
    """
    from collections import defaultdict
    codecm = defaultdict(int)              # codes count map
    for key in hashchain.keys():
        hashmatches = (hashchain[key]["src"] == hcomp["src"],
                       hashchain[key]["arg"] == hcomp["arg"],
                       hashchain[key]["dep"] == hcomp["dep"])
        codecm[hashmatches] += 1

    dists = {k: sum(k)/codecm[k] for k in codecm.keys()}
    sdists = sorted(list(dists.items()), key=lambda x: x[1], reverse=True)

    def ok_or_missing(arg):
        """
        Function that returns 'OK' if the input
        argument is True and 'MISSING' otherwise.
        """
        if arg is True:
            out = "OK"
        elif arg is False:
            out = "MISS"
        else:
            out = "ERROR"
        return out

    logging.info(f"ID:{taskname}, HASH:{htask}")
    msgstr = "  `- src={:>4}, arg={:>4} dep={:>4} has {} candidates."
    for value in sdists:
        code, _ = value
        logging.info(msgstr.format(ok_or_missing(code[0]),
                                   ok_or_missing(code[1]),
                                   ok_or_missing(code[2]),
                                   codecm[code]))
