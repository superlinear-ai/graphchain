"""
Utility functions employed by the graphchain module.
"""
import os
import pickle
import json
import logging
from joblib import hash as joblib_hash
from joblib.func_inspect import get_func_code as joblib_getsource
import fs
import fs.osfs
import fs_s3fs
import lz4.frame
from .logger import init_logging, disable_deps_logging
from .errors import (InvalidPersistencyOption,
                     GraphchainCompressionMismatch,
                     GraphchainPicklingError)


logger = init_logging(name=__name__, logfile="stdout")  # initialize logging


def get_storage(cachedir, persistency, s3bucket=""):
    """
    A function that returns a `fs`-like storage object representing
    the persistency layer of the `hash-chain` cache files. The returned
    object has to be open across the lifetime of the graph optimization.
    """
    assert (isinstance(cachedir, str)
            and isinstance(persistency, str)
            and isinstance(s3bucket, str))

    if persistency == "local":
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir, exist_ok=True)
        storage = fs.osfs.OSFS(os.path.abspath(cachedir))
        return storage
    elif persistency == "s3":
        disable_deps_logging()
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
        logger.error(f"Unrecognized persistency option {persistency}")
        raise InvalidPersistencyOption


def load_hashchain(storage, compression=False):
    """
    Loads the `hash-chain` file found in the root directory of
    the `storage` filesystem object.
    """
    # Constants
    _graphchain = "graphchain.json"
    _cachedir = "cache"

    if not storage.isdir(_cachedir):
        logger.info(f"Creating the cache directory ...")
        storage.makedirs(_cachedir, recreate=True)

    if not storage.isfile(_graphchain):
        logger.info(f"Creating a new hash-chain file {_graphchain}")
        obj = dict()
        write_hashchain(obj, storage, compression=compression)
    else:
        with storage.open(_graphchain, "r") as fid:
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
        filelist_cache = storage.listdir(_cachedir)
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
    _graphchain = "graphchain.json"
    hashchaindata = {"version": str(version),
                     "compression": "lz4" if compression else "none",
                     "hashchain": obj}

    with storage.open(_graphchain, "w") as fid:
        fid.write(json.dumps(hashchaindata, indent=4))


def wrap_to_store(key, obj, storage, objhash,
                  compression=False, skipcache=False):
    """
    Wraps a callable object in order to execute it and store its result.
    """
    def exec_store_wrapper(*args, **kwargs):
        """
        Simple execute and store wrapper.
        """
        _cachedir = "cache"
        if not storage.isdir(_cachedir):
            logging.warning(f"Missing cache directory. Re-creating ...")
            storage.makedirs(_cachedir, recreate=True)

        if callable(obj):
            ret = obj(*args, **kwargs)
            objname = f"key={key} function={obj.__name__}"
        else:
            ret = obj
            objname = f"key={key} constant={str(type(obj))}"

        if compression and not skipcache:
            operation = "EXEC-STORE-COMPRESS"
        elif not compression and not skipcache:
            operation = "EXEC-STORE"
        else:
            operation = "EXEC *ONLY*"
        logger.info(f"* [{objname}] {operation} (hash={objhash})")

        if not skipcache:
            if compression:
                filepath = fs.path.join(_cachedir, objhash + ".pickle.lz4")
                if not storage.isfile(filepath):
                    with storage.open(filepath, "wb") as _fid:
                        with lz4.frame.open(_fid, mode='wb') as fid:
                            try:
                                pickle.dump(ret, fid)
                            except AttributeError as err:
                                logger.error(f"Could not pickle object.")
                                raise GraphchainPicklingError() from err
                else:
                    logger.info(f"`--> * SKIPPING {operation} " +
                                f"(hash={objhash})")
            else:
                filepath = fs.path.join(_cachedir, objhash + ".pickle")
                if not storage.isfile(filepath):
                    with storage.open(filepath, "wb") as fid:
                        try:
                            pickle.dump(ret, fid)
                        except AttributeError as err:
                            logger.error(f"Could not pickle object.")
                            raise GraphchainPicklingError from err
                else:
                    logger.info(f"`-->* SKIPPING {operation} " +
                                f"(hash={objhash})")
        return ret

    return exec_store_wrapper


def wrap_to_load(key, obj, storage, objhash,
                 compression=False):
    """
    Wraps a callable object in order not to execute it and rather
    load its result.
    """
    def loading_wrapper():
        """
        Simple load wrapper.
        """
        _cachedir = "cache"
        assert storage.isdir(_cachedir)

        if compression:
            filepath = fs.path.join(_cachedir, objhash + ".pickle.lz4")
        else:
            filepath = fs.path.join(_cachedir, objhash + ".pickle")

        if callable(obj):
            objname = f"key={key} function={obj.__name__}"
        else:
            objname = f"key={key} constant={str(type(obj))}"

        if compression:
            operation = "LOAD-UNCOMPRESS"
        else:
            operation = "LOAD"
        logger.info(f"* [{objname}] {operation} (hash={objhash})")

        if compression:
            with storage.open(filepath, "rb") as _fid:
                with lz4.frame.open(_fid, mode="r") as fid:
                    ret = pickle.load(fid)
        else:
            with storage.open(filepath, "rb") as fid:
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
                if (isinstance(keyhashmap, dict)
                        and not isinstance(taskelem, list)
                        and not isinstance(taskelem, dict)
                        and taskelem in keyhashmap.keys()):
                    # we have a dask graph key
                    dephash_list.append(keyhashmap[taskelem])
                else:
                    # we have an object of some sort
                    arghash_list.extend(recursive_hash(taskelem))
    else:
        # A non iterable i.e. constant
        arghash_list.extend(recursive_hash(task))

    # Account for the fact that dependencies are also arguments
    arghash_list.append(joblib_hash(joblib_hash(len(dephash_list))))

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
        codecm = defaultdict(int)              # codes count map
        for key in hashchain.keys():
            hashmatches = (hashchain[key]["src"] == hcomp["src"],
                           hashchain[key]["arg"] == hcomp["arg"],
                           hashchain[key]["dep"] == hcomp["dep"])
            codecm[hashmatches] += 1

        dists = {k: sum(k)/codecm[k] for k in codecm.keys()}
        sdists = sorted(list(dists.items()),
                        key=lambda x: x[1],
                        reverse=True)

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

        logger.info(f"* [{taskname}] (hash={htask})")
        msgstr = "`--> HASH MISS: src={:>4}, arg={:>4} " +\
                 "dep={:>4} has {} candidates."
        if sdists:
            for value in sdists:
                code, _ = value
                logger.debug(msgstr.format(ok_or_missing(code[0]),
                                           ok_or_missing(code[1]),
                                           ok_or_missing(code[2]),
                                           codecm[code]))
        else:
            logger.debug(msgstr.format("NONE", "NONE", "NONE", 0))
    else:
        # The key is never cached hence removed from 'graphchain.json'
        logger.info(f"* [{taskname}] (hash={htask})")
        logger.debug("`--> HASH MISS: Non-cachable Key")


def recursive_hash(coll, prev_hash=None):
    """
    Function that recursively hashes collections of objects.
    """
    if prev_hash is None:
        prev_hash = []

    if (not isinstance(coll, list)
            and not isinstance(coll, dict)
            and not isinstance(coll, tuple)
            and not isinstance(coll, set)):
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
