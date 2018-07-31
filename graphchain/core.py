import pickle
import random
import string
from typing import Any, Iterable

import cloudpickle
import dask
import fs
import fs.base
import joblib
import lz4

from .logger import add_logger, mute_dependency_loggers

logger = add_logger(name='graphchain', logfile='stdout')


class CachedComputation:

    def __init__(
            self,
            cachefs: fs.base.FS,
            dsk: dict,
            key: Any,
            computation: Any,
            cache: bool=True) -> None:
        """Cache a dask graph computation.

        Args:
            cachefs: A PyFilesystem such as fs.open_fs('s3://bucket/cache/') to
                store this computation's output in.
            dsk: The dask graph this computation is a part of.
            key: The key corresponding to this computation in the dask graph.
            computation: The computation to cache. See above for the
                allowed computation formats.
            cache: Whether or not to cache this computation.

        Returns:
            CachedComputation: A wrapper for the computation object to replace
                the original computation with in the dask graph.
        """
        self.cachefs = cachefs
        self.dsk = dsk
        self.key = key
        self.computation = computation
        self.cache = cache

    def dependencies(self):
        """Compute all dependencies of this computation."""
        def _dependencies(dsk: dict, key: Any):
            dependencies = dask.core.get_dependencies(dsk, key)
            for dependency in dependencies.copy():
                dependencies |= _dependencies(dsk, dependency)
            return dependencies

        return sorted(_dependencies(self.dsk, self.key), key=str)

    def hash(self):
        """Compute a hash of this computation object and its dependencies."""
        # This CachedComputation's string hash is a combination of its task's
        # hash and its dependencies' (which are also CachedComputations)
        # hashes.
        task_hash = [
            joblib.hash(cloudpickle.dumps(self.computation))
        ]
        deps_hash = [
            # Get the dependency's CachedComputation hash.
            self.dsk[key][0].hash()
            for key in self.dependencies()
        ]
        return joblib.hash(','.join(task_hash + deps_hash))

    def __repr__(self):
        """A string representation of this CachedComputation object."""
        return f'<CachedComputation ' + \
            f'key={self.key} ' + \
            f'task={self.computation} ' + \
            f'dependencies={self.dependencies()}>'

    def __call__(self, *args, **kwargs):
        """Load this computation from cache, or execute and then store it."""
        # Convert key to "POSIX fully portable filename" [1].
        # [1] https://en.wikipedia.org/wiki/Filename
        def _str_to_posix_fully_portable_filename(s):
            safechars = string.ascii_letters + string.digits + '._-'
            return ''.join(c for c in s if c in safechars)

        prefix = _str_to_posix_fully_portable_filename(str(self.key))
        selfhash = self.hash()
        cache_filepath = f'{prefix}-{selfhash}.pickle.lz4'
        # Try to optimistically load from cache.
        # Look for files that match the hash, regardless of the key name.
        cache_candidates = [f for f in self.cachefs.filterdir(
            path='.',
            files=[f'*{selfhash}.pickle.lz4'],
            exclude_dirs=['*'],
            namespaces=['basic']) if f.is_file]
        if cache_candidates:
            logger.info(f'LOAD {self}')
            try:
                with self.cachefs.open(cache_candidates[0].name, 'rb') as fid:
                    with lz4.frame.open(fid, mode='rb') as _fid:
                        return joblib.load(_fid)
            except Exception:
                # Not crucial to stop if we cannot read the file.
                logger.exception('Could not read {cache_filepath}.')
                pass
        # If we couldn't load from cache for some reason, execute and
        # write to cache.
        logger.info(f'EXECUTE {self}')
        if dask.core.istask(self.computation):
            result = self.computation[0](*args, **kwargs)
        else:
            result = args[0]
        if self.cache:
            if not self.cachefs.exists(cache_filepath):
                logger.info(f'STORE {self}')
                cache_filepath_tmp = \
                    f'{cache_filepath}.buffer{random.randint(1000, 9999)}'
                with self.cachefs.open(cache_filepath_tmp, 'wb') as fid:
                    with lz4.frame.open(fid, mode='wb') as _fid:
                        joblib.dump(
                            result, _fid, protocol=pickle.HIGHEST_PROTOCOL)
                try:
                    self.cachefs.move(cache_filepath_tmp, cache_filepath)
                except Exception:
                    # Not crucial to stop if caching fails.
                    logger.exception('Could not write {cache_filepath}.')
                    pass
        return result


def optimize(
    dsk: dict,
    keys: Iterable[Any]=None,
    no_cache_keys: Iterable[Any]=None,
    cachedir="./__graphchain_cache__",
    **kwargs
) -> dict:
    """Optimize a dask graph with cached computations.

    According to the dask graph spec [1], a dask graph is a dictionary that
    maps "keys" to "computations". A computation can be:

        1. Another key in the graph.
        2. A literal.
        3. A task, which is of the form (callable, *args).
        4. A list of other computations.

    This optimizer replaces all computations in a graph with
    CachedComputations, so that getting items from the graph will be backed by
    a cache of your choosing.

    CachedComputation objects _do not_ hash task inputs (which is what
    functools.lru_cache and joblib.Memory do). Instead, they build a chain
    of hashes (hence graphchain) of the computation and its dependencies.
    If the computation or its dependencies change, that will invalidate the
    computation's cache and a new one will need to be computed and stored.

    Args:
        dsk: The dask graph to optimize with caching computations.
        key: Not used. Is present for compatibility with dask optimizers [2].
        no_cache_keys: An iterable of keys not to cache.
        cachedir: A PyFilesystem FS URL to store the cached computations in.
            Can be a local directory such as './__graphchain_cache__' or a
            remote directory such as 's3://bucket/__graphchain_cache__'.

    Returns:
        dict: A copy of the dask graph where the computations have been
            replaced by CachedComputations.

    [1] http://dask.pydata.org/en/latest/spec.html
    [2] http://dask.pydata.org/en/latest/optimize.html
    """
    # Verify that the graph is a DAG.
    dsk = dsk.copy()
    assert dask.core.isdag(dsk, list(dsk.keys()))
    # create=True does not yet work for S3FS [1]. This should probably be left
    # to the user as we don't know in which region to create the bucket, among
    # other configuration options.
    # [1] https://github.com/PyFilesystem/s3fs/issues/23
    mute_dependency_loggers()
    cachefs = fs.open_fs(cachedir, create=True)
    # Replace graph computations by CachedComputations.
    no_cache_keys = no_cache_keys or set()
    for key in dsk:
        computation = dsk[key]
        cc = CachedComputation(
            cachefs, dsk, key, computation,
            cache=key not in no_cache_keys)
        dsk[key] = \
            (cc,) + computation[1:] if dask.core.istask(computation) else \
            (cc, computation)
    return dsk
