"""
Utility functions employed by the graphchain module.
"""
import json

import fs
import joblib
import lz4.frame
from joblib import hash as joblib_hash
from joblib.func_inspect import get_func_code as joblib_getsource
from typing import Union
from dask.core import get_dependencies

from .errors import GraphchainCompressionMismatch
from .logger import add_logger, mute_dependency_loggers

logger = add_logger(name="graphchain", logfile="stdout")
GRAPHCHAIN_FILENAME = "graphchain.json"
CACHE_DIRNAME = "cache"


def get_storage(cachedir):
    """Open a PyFilesystem filesystem given an FS URL.

    Args:
        cachedir (str): The FS URL to open. Can be a local directory such as
            "./__graphchain_cache__" or a remote directory such as
            "s3://mybucket/__graphchain_cache__".

    Returns:
        fs.FS: A PyFilesystem filesystem object that has created the directory
            structure.
    """
    mute_dependency_loggers()
    storage = fs.open_fs(cachedir, create=True)
    return storage


def load_hashchain(storage, compress=False):
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
        write_hashchain(obj, storage, compress=compress)
    else:
        with storage.open(GRAPHCHAIN_FILENAME, "r") as fid:
            hashchaindata = json.load(fid)

        # Check compression option consistency
        compr_option_lz4 = hashchaindata["compress"] == "lz4"
        if compr_option_lz4 ^ compress:
            raise GraphchainCompressionMismatch(
                f"compress option mismatch: "
                f"file={compr_option_lz4}, "
                f"optimizer={compress}.")

        # Prune hashchain based on cache
        obj = hashchaindata["hashchain"]
        filelist_cache = storage.listdir(CACHE_DIRNAME)
        hash_list = {_file.split(".")[0] for _file in filelist_cache}
        to_delete = set(obj.keys()) - hash_list
        for _hash in to_delete:
            del obj[_hash]
        write_hashchain(obj, storage, compress=compress)

    return obj


def write_hashchain(obj, storage, version=1, compress=False):
    """
    Writes a `hash-chain` contained in ``obj`` to a file
    indicated by ``filename``.
    """
    hashchaindata = {
        "version": str(version),
        "compress": "lz4" if compress else "none",
        "hashchain": obj
    }
    with storage.open(GRAPHCHAIN_FILENAME, "w") as fid:
        fid.write(json.dumps(hashchaindata, indent=4))


def wrap_to_store(key,
                  obj,
                  storage,
                  objhash,
                  compress=False,
                  skipcache=False):
    """
    Wraps a callable object in order to execute it and store its result.
    """
    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        if callable(obj):
            objname = f"key={key} function={obj.__name__} hash={objhash}"
        else:
            objname = f"key={key} literal={type(obj)} hash={objhash}"
        logger.info(f"EXECUTE {objname}")
        ret = obj(*args, **kwargs) if callable(obj) else obj
        if not skipcache:
            fileext = ".pickle.lz4" if compress else ".pickle"
            filepath = fs.path.join(CACHE_DIRNAME, objhash + fileext)
            if not storage.isfile(filepath):
                logger.info(f"STORE {objname}")
                try:
                    with storage.open(filepath, "wb") as fid:
                        if compress:
                            with lz4.frame.open(fid, mode='wb') as _fid:
                                joblib.dump(ret, _fid)
                        else:
                            joblib.dump(ret, fid)
                except Exception:
                    logger.exception("Could not dump object.")
            else:
                logger.warning(f"FILE_EXISTS {objname}")
        return ret

    return exec_store_wrapper


def wrap_to_load(key, obj, storage, objhash, compress=False):
    """
    Wraps a callable object in order not to execute it and rather
    load its result.
    """
    def loading_wrapper():
        """
        Simple load wrapper.
        """
        assert storage.isdir(CACHE_DIRNAME)
        filepath = fs.path.join(
            CACHE_DIRNAME, f"{objhash}.pickle{'.lz4' if compress else ''}")
        if callable(obj):
            objname = f"key={key} function={obj.__name__} hash={objhash}"
        else:
            objname = f"key={key} literal={type(obj)} hash={objhash}"
        logger.info(f"LOAD {objname}")
        with storage.open(filepath, "rb") as fid:
            if compress:
                with lz4.frame.open(fid, mode="r") as _fid:
                    ret = joblib.load(_fid)
            else:
                ret = joblib.load(fid)
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


def get_bottom_tasks(dsk: dict,
                     task: Union[str, list, dict]) -> Union[str, list, dict]:
    """
    Function that iteratively replaces any task graph keys present in
    an input variable `task` with the lowest level keys in the task graph
    `dsk` (i.e. the ones pointing to the actual values). This allows
    """
    if isinstance(task, str) and task in dsk.keys():
        if not get_dependencies(dsk, task):
            return task
        else:
            task = get_bottom_tasks(dsk, dsk[task])
    elif isinstance(task, list):
        for idx in range(len(task)):
            task[idx] = get_bottom_tasks(dsk, task[idx])
    elif isinstance(task, dict):
        for key in task:
            task[key] = get_bottom_tasks(dsk, task[key])
    else:
        # Non-key of collection, return value as is
        pass

    return task
