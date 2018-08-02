import functools
import pickle
import random
import time
from typing import \
    Any, Callable, Container, Hashable, Iterable, List, Optional, Union

import cloudpickle
import dask
import fs
import fs.base
import fs.info
import joblib
import lz4

from .logger import add_logger, mute_dependency_loggers
from .utils import get_size, str_to_posix_fully_portable_filename

logger = add_logger(name='graphchain', logfile='stdout')


class CachedComputation:

    def __init__(
            self,
            cachefs: fs.base.FS,
            dsk: dict,
            key: Hashable,
            computation: Any,
            write_to_cache: Union[bool, str]='auto') -> None:
        """Cache a dask graph computation.

        Args:
            cachefs: A PyFilesystem such as fs.open_fs('s3://bucket/cache/') to
                store this computation's output in.
            dsk: The dask graph this computation is a part of.
            key: The key corresponding to this computation in the dask graph.
            computation: The computation to cache. See above for the
                allowed computation formats.
            write_to_cache: Whether or not to cache this computation. If set
                to 'auto', will only write to cache if it is expected this will
                speed up future gets of this computation.

        Returns:
            CachedComputation: A wrapper for the computation object to replace
                the original computation with in the dask graph.
        """
        self.cachefs = cachefs
        self.dsk = dsk
        self.key = key
        self.computation = computation
        self.write_to_cache = write_to_cache

    def __repr__(self):
        """A string representation of this CachedComputation object."""
        return f'<CachedComputation key={self.key} task={self.computation}>'

    @property  # type: ignore
    @functools.lru_cache()
    def hash(self) -> str:
        """Compute a hash of this computation object and its dependencies."""
        # Replace references in this computation to other tasks by their
        # hashes.
        computation = self.computation
        for dep in dask.core.get_dependencies(self.dsk, task=computation):
            computation = dask.core.subs(
                computation,
                dep,
                self.dsk[dep].hash
                if isinstance(self.dsk[dep], CachedComputation)
                else self.dsk[dep][0].hash)
        # Return the hash of the resulting computation.
        return joblib.hash(cloudpickle.dumps(computation))

    @staticmethod
    def estimated_load_time(result):
        """The estimated time to load the given result from cache."""
        compression_ratio = 2
        size = get_size(result) / compression_ratio
        # Default to typical SSD latency and maximum EBS read throughput.
        read_latency = dask.config.get('cache_latency', 1e-4)
        read_throughput = dask.config.get('cache_throughput', 500e6)
        return read_latency + size / read_throughput

    @functools.lru_cache()
    def read_time(self, cache_filename, timing_type):
        """Read the time to load, compute, or store from file."""
        time_filename = cache_filename + '.time.' + timing_type
        with self.cachefs.open(time_filename, 'r') as fid:
            return float(fid.read())

    def time_to_result(self, memoize: bool=True) -> float:
        """The expected time to load or compute this computation."""
        if hasattr(self, '_time_to_result'):
            return self._time_to_result  # type: ignore
        if memoize:
            try:
                cache_filename = self.cache_filename(verify=True)
                try:
                    load_time = self.read_time(cache_filename, 'load')
                except Exception:
                    load_time = self.read_time(cache_filename, 'store') / 2
                self._time_to_result = load_time
                return load_time
            except Exception:
                pass
        cache_filename = self.cache_filename(verify=False)
        compute_time = self.read_time(cache_filename, 'compute')
        dependency_time = 0
        for dep in dask.core.get_dependencies(
                self.dsk, task=self.computation):
            dependency_time += self.dsk[dep][0].time_to_result()
        total_time = compute_time + dependency_time
        if memoize:
            self._time_to_result = total_time
        return total_time

    def cache_candidates(self) -> List[fs.info.Info]:
        """Get a list of cache candidates to load this computation from."""
        # Look for files that match the hash, regardless of the key name.
        return [
            f for f in self.cachefs.filterdir(
                path='.',
                files=[f'*{self.hash}.pickle.lz4'],
                exclude_dirs=['*'],
                namespaces=['basic', 'details'])
            if f.is_file
        ]

    def cache_filename(self, verify: bool) -> Optional[str]:
        """Filename of the cache file to load or store."""
        if verify:
            cache_candidates = self.cache_candidates()
            if cache_candidates:
                return cache_candidates[0].name
            return None
        prefix = str_to_posix_fully_portable_filename(str(self.key))
        return f'{prefix}-{self.hash}.pickle.lz4'

    def load(self, cache_filename: Optional[str]=None) -> Any:
        """Load this result of this computation from cache."""
        cache_filename = cache_filename or self.cache_filename(verify=True)
        try:
            # Load from cache.
            start_time = time.time()
            logger.info(
                f'LOAD {self} from {self.cachefs}/{cache_filename}')
            with self.cachefs.open(cache_filename, 'rb') as fid:
                with lz4.frame.open(fid, mode='rb') as _fid:
                    result = joblib.load(_fid)
            load_time = time.time() - start_time
            # Write load time.
            time_filename = cache_filename + '.time.load'  # type: ignore
            with self.cachefs.open(time_filename, 'w') as fid:
                fid.write(str(load_time))
            return result
        except Exception:
            logger.exception('Could not read {cache_filename}.')
            raise

    def compute(
            self, cache_filename: Optional[str]=None, *args, **kwargs) -> Any:
        """Compute this computation."""
        # Compute the computation.
        start_time = time.time()
        logger.info(f'COMPUTE {self}')
        if dask.core.istask(self.computation):
            result = self.computation[0](*args, **kwargs)
        else:
            result = args[0]
        compute_time = time.time() - start_time
        # Write compute time if there's a cache filename.
        if cache_filename:
            time_filename = cache_filename + '.time.compute'
            with self.cachefs.open(time_filename, 'w') as fid:
                fid.write(str(compute_time))
        return result

    def store(self, result: Any, cache_filename: Optional[str]=None) -> None:
        """Store the result of this computation in the cache."""
        cache_filename = cache_filename or self.cache_filename(verify=False)
        if not self.cachefs.exists(cache_filename):
            logger.info(f'STORE {self} to {self.cachefs}/{cache_filename}')
            tmp = f'{cache_filename}.buffer{random.randint(1000, 9999)}'
            try:
                # Store to cache.
                start_time = time.time()
                with self.cachefs.open(tmp, 'wb') as fid:
                    with lz4.frame.open(fid, mode='wb') as _fid:
                        joblib.dump(
                            result, _fid, protocol=pickle.HIGHEST_PROTOCOL)
                self.cachefs.move(tmp, cache_filename)
                store_time = time.time() - start_time
                # Write store time.
                time_filename = cache_filename + '.time.store'  # type: ignore
                with self.cachefs.open(time_filename, 'w') as fid:
                    fid.write(str(store_time))
            except Exception:
                # Not crucial to stop if caching fails.
                logger.exception('Could not write {cache_filename}.')
                try:
                    self.cachefs.remove(tmp)
                except Exception:
                    pass

    def patch_computation_in_graph(self):
        if self.cache_candidates():
            # If there are cache candidates to load this computation from,
            # remove all dependencies for this task from the graph as far as
            # dask is concerned.
            self.dsk[self.key] = (self,)
        else:
            # If there are no cache candidates, wrap the execution of the
            # computation with this CachedComputation's __call__ method and
            # keep references to its dependencies.
            self.dsk[self.key] = \
                (self,) + self.computation[1:] \
                if dask.core.istask(self.computation) else \
                (self, self.computation)

    def __call__(self, *args, **kwargs):
        """Load this computation from cache, or compute and then store it."""
        # Load.
        cache_filename = self.cache_filename(verify=True)
        if cache_filename:
            return self.load(cache_filename)
        # Compute.
        cache_filename = self.cache_filename(verify=False)
        result = self.compute(cache_filename, *args, **kwargs)
        # Store.
        write_to_cache = self.write_to_cache
        if write_to_cache == 'auto':
            compute_time = self.time_to_result(memoize=False)
            estimated_load_time = CachedComputation.estimated_load_time(result)
            write_to_cache = estimated_load_time < compute_time
            logger.info(
                f'{"Caching" if write_to_cache else "Not caching"} {self} '
                f'because estimated_load_time={estimated_load_time} '
                f'{"<" if write_to_cache else ">="} '
                f'compute_time={compute_time}')
        if write_to_cache:
            self.store(result, cache_filename)
        return result


def optimize(
        dsk: dict,
        keys: Optional[Iterable[Hashable]]=None,
        no_cache_keys: Optional[Container[Hashable]]=None,
        cachedir: str="./__graphchain_cache__",
        **kwargs) -> dict:
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
        keys: Not used. Is present for compatibility with dask optimizers [2].
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
        dsk[key] = CachedComputation(
            cachefs, dsk, key, computation,
            write_to_cache=False if key in no_cache_keys else 'auto')
    # Remove task arguments if we can load from cache.
    for key in dsk:
        dsk[key].patch_computation_in_graph()
    return dsk


def get(
        dsk: dict,
        keys: Iterable[Hashable],
        scheduler: Optional[Callable]=None,
        **kwargs):
    """A cache-optimized equivalent to dask.get.

    Optimizes a dask graph with graphchain.optimize and then computes the
    requested keys with the desired scheduler, which is by default dask.get.

    Args:
        dsk: The dask graph to query.
        keys: The keys to compute.
        scheduler (optional): The dask scheduler to use to retrieve the keys
            from the graph.
        **kwargs (optional) Keyword arguments for the 'optimize' function;
            can be any of the following: 'no_cache_keys', 'cachedir'.

    Returns:
        The computed values corresponding to the given keys.
    """
    cached_dsk = optimize(dsk, keys, **kwargs)
    scheduler = \
        scheduler or \
        dask.config.get('get', None) or \
        dask.config.get('scheduler', None) or \
        dask.get
    return scheduler(cached_dsk, keys)
