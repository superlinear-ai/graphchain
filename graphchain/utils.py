import inspect
import string
import sys
from typing import Any, Optional


def _fast_get_size(obj: Any) -> int:
    if hasattr(obj, 'sample') and hasattr(obj, 'memory_usage'):  # DF, Series.
        n = min(len(obj), 1000)
        s = obj.sample(n=n).memory_usage(index=True, deep=True)
        if hasattr(s, 'sum'):
            s = s.sum()
        if hasattr(s, 'compute'):
            s = s.compute()
        s = s / n * len(obj)
        return s
    elif hasattr(obj, 'nbytes'):  # Numpy.
        return obj.nbytes
    elif hasattr(obj, 'data') and hasattr(obj.data, 'nbytes'):  # Sparse.
        return 3 * obj.data.nbytes
    raise TypeError('Could not determine size of the given object.')


def _slow_get_size(obj: Any, seen: set) -> int:
    size = sys.getsizeof(obj)
    if hasattr(obj, '__dict__'):
        for cls in obj.__class__.__mro__:
            if '__dict__' in cls.__dict__:
                d = cls.__dict__['__dict__']
                if inspect.isgetsetdescriptor(d) \
                        or inspect.ismemberdescriptor(d):
                    size += get_size(obj.__dict__, seen)
                break
    if isinstance(obj, dict):
        size += sum((get_size(v, seen) for v in obj.values()))
        size += sum((get_size(k, seen) for k in obj.keys()))
    elif hasattr(obj, '__iter__') \
            and not isinstance(obj, (str, bytes, bytearray)):
        size += sum((get_size(i, seen) for i in obj))
    if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
        size += sum(get_size(getattr(obj, s), seen)
                    for s in obj.__slots__ if hasattr(obj, s))
    return size


def get_size(obj: Any, seen: Optional[set]=None) -> int:
    """Recursively finds size of objects in bytes, based on [1].
    [1] https://github.com/bosswissam/pysize
    """
    # Keep track of the seen set.
    seen = seen or set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    # Short-circuit some types.
    try:
        return _fast_get_size(obj)
    except TypeError:
        pass
    # General-purpose size computation.
    return _slow_get_size(obj, seen)


def str_to_posix_fully_portable_filename(s: str) -> str:
    """Convert key to POSIX fully portable filename [1].
    [1] https://en.wikipedia.org/wiki/Filename
    """
    safechars = string.ascii_letters + string.digits + '._-'
    return ''.join(c for c in s if c in safechars)
