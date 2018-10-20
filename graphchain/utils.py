"""Utility functions used by graphchain."""
import string
import sys
from typing import Any, Optional


def _fast_get_size(obj: Any) -> int:
    if hasattr(obj, '__len__') and len(obj) <= 0:
        return 0
    if hasattr(obj, 'sample') and hasattr(obj, 'memory_usage'):  # DF, Series.
        n = min(len(obj), 1000)
        s = obj.sample(n=n).memory_usage(index=True, deep=True)
        if hasattr(s, 'sum'):
            s = s.sum()
        if hasattr(s, 'compute'):
            s = s.compute()
        s = s / n * len(obj)
        return int(s)
    elif hasattr(obj, 'nbytes'):  # Numpy.
        return int(obj.nbytes)
    elif hasattr(obj, 'data') and hasattr(obj.data, 'nbytes'):  # Sparse.
        return int(3 * obj.data.nbytes)
    raise TypeError('Could not determine size of the given object.')


def _slow_get_size(obj: Any, seen: Optional[set]=None) -> int:
    size = sys.getsizeof(obj)
    seen = seen or set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum(get_size(v, seen) for v in obj.values())
        size += sum(get_size(k, seen) for k in obj.keys())
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and \
            not isinstance(obj, (str, bytes, bytearray)):
        size += sum(get_size(i, seen) for i in obj)
    return size


def get_size(obj: Any, seen: Optional[set]=None) -> int:
    """Recursively compute the size of an object.

    Parameters
    ----------
        obj
            The object to get the size of.

    Returns
    -------
        int
            The (approximate) size in bytes of the given object.
    """
    # Short-circuit some types.
    try:
        return _fast_get_size(obj)
    except TypeError:
        pass
    # General-purpose size computation.
    return _slow_get_size(obj, seen)


def str_to_posix_fully_portable_filename(s: str) -> str:
    """Convert key to POSIX fully portable filename [1].

    Parameters
    ----------
        s
            The string to convert to a POSIX fully portable filename.

    Returns
    -------
        str
            A POSIX fully portable filename.

    References
    ----------
    .. [1] https://en.wikipedia.org/wiki/Filename
    """
    safechars = string.ascii_letters + string.digits + '._-'
    return ''.join(c if c in safechars else '-' for c in s)
