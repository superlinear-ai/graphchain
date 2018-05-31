"""
Utility functions employed by the graphchain module.
"""
import json
import os
import pickle
import sys

import fs
import fs.osfs
import fs_s3fs
import lz4.frame
from joblib import hash as joblib_hash
from joblib.func_inspect import get_func_code as joblib_getsource

from .errors import (GraphchainCompressionMismatch, GraphchainPicklingError,
                     InvalidPersistencyOption)
from .logger import add_logger, mute_dependency_loggers

logger = add_logger(name="graphchain", logfile="stdout")
GRAPHCHAIN_FILENAME = "graphchain.json"
CACHE_DIRNAME = "cache"


def get_storage(cachedir, persistency, s3bucket=None):
    """
    A function that returns a `fs`-like storage object representing
    the persistency layer of the `hash-chain` cache files. The returned
    object has to be open across the lifetime of the graph optimization.
    """
    assert isinstance(cachedir, str) and isinstance(persistency, str)
    if persistency == "local":
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir, exist_ok=True)
        storage = fs.osfs.OSFS(os.path.abspath(cachedir))
        return storage
    elif persistency == "s3":
        assert isinstance(s3bucket, str)
        mute_dependency_loggers()
        try:
            _storage = fs_s3fs.S3FS(s3bucket)
            if not _storage.isdir(cachedir):
                _storage.makedirs(cachedir, recreate=True)
            _storage.close()
            storage = fs_s3fs.S3FS(s3bucket, cachedir)
            return storage
        except Exception:
            # Something went wrong (probably) in the S3 access
            logger.error("Error encountered in S3 access "
                         f"(bucket='{s3bucket}')")
            raise
    else:
        logger.error(f"Unrecognized persistency option {persistency}.")
        raise InvalidPersistencyOption


def load_hashchain(storage, compression=False):
    """
    Loads the `hash-chain` file found in the root directory of
    the `storage` filesystem object.
    """
    if not storage.isdir(CACHE_DIRNAME):
        logger.info(f"Initializing {CACHE_DIRNAME}")
        storage.makedirs(CACHE_DIRNAME, recreate=True)

    if not storage.isfile(GRAPHCHAIN_FILENAME):
        logger.info(f"Initializing {GRAPHCHAIN_FILENAME}")
        obj = dict()
        write_hashchain(obj, storage, compression=compression)
    else:
        with storage.open(GRAPHCHAIN_FILENAME, "r") as fid:
            hashchaindata = json.load(fid)

        # Check compression option consistency
        compr_option_lz4 = hashchaindata["compression"] == "lz4"
        if compr_option_lz4 ^ compression:
            raise GraphchainCompressionMismatch(
                f"Compression option mismatch: "
                f"file={compr_option_lz4}, "
                f"optimizer={compression}.")

        # Prune hashchain based on cache
        obj = hashchaindata["hashchain"]
        filelist_cache = storage.listdir(CACHE_DIRNAME)
        hash_list = {_file.split(".")[0] for _file in filelist_cache}
        to_delete = set(obj.keys()) - hash_list
        for _hash in to_delete:
            del obj[_hash]
        write_hashchain(obj, storage, compression=compression)

    return obj


def write_hashchain(obj, storage, version=1, compression=False):
    """
    Writes a `hash-chain` contained in ``obj`` to a file
    indicated by ``filename``.
    """
    hashchaindata = {
        "version": str(version),
        "compression": "lz4" if compression else "none",
        "hashchain": obj
    }
    with storage.open(GRAPHCHAIN_FILENAME, "w") as fid:
        fid.write(json.dumps(hashchaindata, indent=4))


def _pickle_dump(storage, compression, filepath, obj):
    if sys.platform != "darwin":
        # Non-macOS system, write the usual way.
        with storage.open(filepath, "wb") as fid:
            try:
                if compression:
                    with lz4.frame.open(fid, mode='wb') as _fid:
                        pickle.dump(obj, _fid)
                else:
                    pickle.dump(obj, fid)
            except AttributeError as err:
                logger.error(f"Could not pickle object.")
                raise GraphchainPicklingError() from err
    else:
        # MacOS, split files into 2GB chunks. Note: this bit should be removed
        # once the pickle bug [1] is fixed as it is ineficient from a
        # memory-usage standpoint.
        #
        # [1] https://bugs.python.org/issue24658
        max_bytes = 2**31 - 1
        bytes_out = pickle.dumps(obj)
        n_bytes = sys.getsizeof(bytes_out)
        with storage.open(filepath, "wb") as fid:
            try:
                if compression:
                    with lz4.frame.open(fid, mode='wb') as _fid:
                        for idx in range(0, n_bytes, max_bytes):
                            _fid.write(bytes_out[idx:idx + max_bytes])
                else:
                    for idx in range(0, n_bytes, max_bytes):
                        fid.write(bytes_out[idx:idx + max_bytes])
            except AttributeError as err:
                logger.error(f"Could not pickle object.")
                raise GraphchainPicklingError() from err


def wrap_to_store(key,
                  obj,
                  storage,
                  objhash,
                  compression=False,
                  skipcache=False):
    """
    Wraps a callable object in order to execute it and store its result.
    """

    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        if callable(obj):
            ret = obj(*args, **kwargs)
            objname = f"key={key} function={obj.__name__}"
        else:
            ret = obj
            objname = f"key={key} literal={type(obj)}"
        operation = "EXECUTE + STORE" if not skipcache else "EXECUTE"
        logger.info(f"{operation} {objname}")
        fileext = ".pickle.lz4" if compression else ".pickle"
        filepath = fs.path.join(CACHE_DIRNAME, objhash + fileext)
        if not skipcache:
            if not storage.isfile(filepath):
                _pickle_dump(storage, compression, filepath, ret)
            else:
                logger.info(f"SKIPPING STORE {objname}")
        return ret

    return exec_store_wrapper


def wrap_to_load(key, obj, storage, objhash, compression=False):
    """
    Wraps a callable object in order not to execute it and rather
    load its result.
    """
    global CACHE_DIRNAME

    def loading_wrapper():
        """
        Simple load wrapper.
        """
        assert storage.isdir(CACHE_DIRNAME)
        filepath = fs.path.join(
            CACHE_DIRNAME, f"{objhash}.pickle{'.lz4' if compression else ''}")

        if callable(obj):
            objname = f"key={key} function={obj.__name__}"
        else:
            objname = f"key={key} literal={str(type(obj))}"
        logger.info(f"LOAD {objname}")

        with storage.open(filepath, "rb") as fid:
            if compression:
                with lz4.frame.open(fid, mode="r") as _fid:
                    ret = pickle.load(_fid)
            else:
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
    # assert task is not None
    fnhash_list = []
    arghash_list = []
    dephash_list = []

    if isinstance(task, tuple):
        # A tuple would correspond to a delayed function
        for taskelem in task:
            if callable(taskelem):
                # function
                sourcecode = joblib_getsource(taskelem)[0]
                fnhash_list.append(joblib_hash(sourcecode))
            else:
                try:
                    # Assume a dask graph key.
                    dephash_list.append(keyhashmap[taskelem])
                except Exception:
                    # Else hash the object.
                    arghash_list.extend(recursive_hash(taskelem))
    else:
        try:
            # Assume a dask graph key.
            dephash_list.append(keyhashmap[task])
        except Exception:
            # Else hash the object.
            arghash_list.extend(recursive_hash(task))

    # Calculate subhashes
    src_hash = joblib_hash("".join(fnhash_list))
    arg_hash = joblib_hash("".join(arghash_list))
    dep_hash = joblib_hash("".join(dephash_list))

    subhashes = {"src": src_hash, "arg": arg_hash, "dep": dep_hash}
    objhash = joblib_hash(src_hash + arg_hash + dep_hash)
    return objhash, subhashes


def analyze_hash_miss(hashchain, htask, hcomp, taskname, skipcache):
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
    if not skipcache:
        from collections import defaultdict
        codecm = defaultdict(int)  # codes count map
        for key in hashchain.keys():
            hashmatches = (hashchain[key]["src"] == hcomp["src"],
                           hashchain[key]["arg"] == hcomp["arg"],
                           hashchain[key]["dep"] == hcomp["dep"])
            codecm[hashmatches] += 1

        dists = {k: sum(k) / codecm[k] for k in codecm.keys()}
        sdists = sorted(list(dists.items()), key=lambda x: x[1], reverse=True)

        def ok_or_missing(arg):
            """
            Function that returns 'OK' if the input
            argument is True and 'MISSING' otherwise.
            """
            if arg is True:
                out = "HIT"
            elif arg is False:
                out = "MISS"
            else:
                out = "ERROR"
            return out

        msgstr = f"CACHE MISS for key={taskname} with " + \
            "src={:>4} arg={:>4} dep={:>4} ({} candidates)"
        if sdists:
            for value in sdists:
                code, _ = value
                logger.debug(
                    msgstr.format(
                        ok_or_missing(code[0]), ok_or_missing(code[1]),
                        ok_or_missing(code[2]), codecm[code]))
        else:
            logger.debug(msgstr.format("NONE", "NONE", "NONE", 0))
    else:
        # The key is never cached hence removed from 'graphchain.json'
        logger.debug(f"CACHE SKIPPED for key={taskname}")


def recursive_hash(coll, prev_hash=None):
    """
    Function that recursively hashes collections of objects.
    """
    if prev_hash is None:
        prev_hash = []

    if (not isinstance(coll, list) and not isinstance(coll, dict)
            and not isinstance(coll, tuple) and not isinstance(coll, set)):
        if callable(coll):
            prev_hash.append(joblib_hash(joblib_getsource(coll)[0]))
        else:
            prev_hash.append(joblib_hash(coll))
    elif isinstance(coll, dict):
        # Special case for dicts: inspect both keys and values
        for (key, val) in coll.items():
            recursive_hash(key, prev_hash)
            recursive_hash(val, prev_hash)
    else:
        for val in coll:
            recursive_hash(val, prev_hash)

    return prev_hash
