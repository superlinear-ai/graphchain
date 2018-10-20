"""Test module for the graphchain core."""
import functools
import os
import shutil
import tempfile
from typing import Callable, Tuple

import dask
import fs
import pytest

from ..core import CachedComputation, optimize


@pytest.fixture(scope="function")  # type: ignore
def dask_graph() -> dict:
    r"""Generate an example dask graph.

    Will be used as a basis for the functional testing of the graphchain
    module:

                     O top(..)
                 ____|____
                /         \
               d1          O baz(..)
                  _________|________
                 /                  \
                O boo(...)           O goo(...)
         _______|_______         ____|____
        /       |       \       /    |    \
       O        O        O     O     |     O
     foo(.) bar(.)    baz(.)  foo(.) v6  bar(.)
      |         |        |     |           |
      |         |        |     |           |
      v1       v2       v3    v4          v5
    """
    # Functions
    def foo(argument: int) -> int:
        return argument

    def bar(argument: int) -> int:
        return argument + 2

    def baz(*args: int) -> int:
        return sum(args)

    def boo(*args: int) -> int:
        return len(args) + sum(args)

    def goo(*args: int) -> int:
        return sum(args) + 1

    def top(argument: int, argument2: int) -> int:
        return argument - argument2

    # Graph (for the function definitions above)
    dsk = {
        "v0": None,
        "v1": 1,
        "v2": 2,
        "v3": 3,
        "v4": 0,
        "v5": -1,
        "v6": -2,
        "d1": -3,
        "foo1": (foo, "v1"),
        "foo2": (foo, "v4"),
        "bar1": (bar, "v2"),
        "bar2": (bar, "v5"),
        "baz1": (baz, "v3"),
        "baz2": (baz, "boo1", "goo1"),
        "boo1": (boo, "foo1", "bar1", "baz1"),
        "goo1": (goo, "foo2", "bar2", "v6"),
        "top1": (top, "d1", "baz2")
    }
    return dsk


@pytest.fixture(scope="module")  # type: ignore
def temp_dir() -> str:
    """Create a temporary directory to store the cache in."""
    with tempfile.TemporaryDirectory(prefix='__graphchain_cache__') as tmpdir:
        yield tmpdir


@pytest.fixture(scope="module")  # type: ignore
def temp_dir_s3() -> str:
    """Create the directory used for the graphchain tests on S3."""
    location = "s3://graphchain-test-bucket/__pytest_graphchain_cache__"
    yield location


def test_dag(dask_graph: dict) -> None:
    """Test that the graph can be traversed and its result is correct."""
    dsk = dask_graph
    result = dask.get(dsk, ["top1"])
    assert result == (-14, )


@pytest.fixture(scope="function")  # type: ignore
def optimizer(temp_dir: str) -> Tuple[str, Callable]:
    """Prefill the graphchain optimizer's parameters."""
    return temp_dir, functools.partial(
        optimize,
        keys=('top1',),
        location=temp_dir)


@pytest.fixture(scope="function")  # type: ignore
def optimizer_exec_only_nodes(temp_dir: str) -> Tuple[str, Callable]:
    """Prefill the graphchain optimizer's parameters."""
    return temp_dir, functools.partial(
        optimize,
        keys=('top1',),
        location=temp_dir,
        skip_keys=['boo1'])


@pytest.fixture(scope="function")  # type: ignore
def optimizer_s3(temp_dir_s3: str) -> Tuple[str, Callable]:
    """Prefill the graphchain optimizer's parameters."""
    return temp_dir_s3, functools.partial(
        optimize,
        keys=('top1',),
        location=temp_dir_s3)


def test_first_run(dask_graph: dict, optimizer: Tuple[str, Callable]) -> None:
    """First run.

    Tests a first run of the graphchain optimization function ``optimize``. It
    checks the final result, that that all function calls are wrapped - for
    execution and output storing, that the hashchain is created, that hashed
    outputs (the <hash>.pickle[.lz4] files) are generated and that the name of
    each file is a key in the hashchain.
    """
    dsk = dask_graph
    cache_dir, graphchain_optimize = optimizer

    # Run optimizer
    newdsk = graphchain_optimize(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14, )

    # Check that all functions have been wrapped
    for key, _task in dsk.items():
        newtask = newdsk[key]
        assert isinstance(newtask[0], CachedComputation)

    # Check that the hash files are written and that each
    # filename can be found as a key in the hashchain
    # (the association of hash <-> DAG tasks is not tested)
    storage = fs.osfs.OSFS(cache_dir)
    filelist = storage.listdir("/")
    nfiles = len(filelist)
    assert nfiles >= len(dsk)
    storage.close()


def NO_test_single_run_s3(
        dask_graph: dict,
        optimizer_s3: Tuple[str, Callable]) -> None:
    """Run on S3.

    Tests a single run of the graphchain optimization function ``optimize``
    using Amazon S3 as a persistency layer. It checks the final result, that
    all function calls are wrapped - for execution and output storing, that the
    hashchain is created, that hashed outputs (the <hash>.pickle[.lz4] files)
    are generated and that the name of each file is a key in the hashchain.
    """
    dsk = dask_graph
    cache_dir, graphchain_optimize = optimizer_s3

    # Run optimizer
    newdsk = graphchain_optimize(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14, )

    data_ext = ".pickle.lz4"

    # Check that all functions have been wrapped
    for key, _task in dsk.items():
        newtask = newdsk[key]
        isinstance(newtask, CachedComputation)

    # Check that the hash files are written and that each
    # filename can be found as a key in the hashchain
    # (the association of hash <-> DAG tasks is not tested)
    storage = fs.open_fs(cache_dir)
    filelist = storage.listdir("/")
    nfiles = sum(map(lambda x: x.endswith(data_ext), filelist))
    assert nfiles == len(dsk)


def test_second_run(
        dask_graph: dict,
        optimizer: Tuple[str, Callable]) -> None:
    """Second run.

    Tests a second run of the graphchain optimization function `optimize`. It
    checks the final result, that that all function calls are wrapped - for
    loading and the the result key has no dependencies.
    """
    dsk = dask_graph
    _, graphchain_optimize = optimizer

    # Run optimizer
    newdsk = graphchain_optimize(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14, )

    # Check that the functions are wrapped for loading
    for key in dsk.keys():
        newtask = newdsk[key]
        assert isinstance(newtask, tuple)
        assert isinstance(newtask[0], CachedComputation)


def test_node_changes(
        dask_graph: dict,
        optimizer: Tuple[str, Callable]) -> None:
    """Test node changes.

    Tests the functionality of the graphchain in the event of changes in the
    structure of the graph, namely by altering the functions/constants
    associated to the tasks. After optimization, the afected nodes should be
    wrapped in a storeand execution wrapper and their dependency lists should
    not be empty.
    """
    dsk = dask_graph
    _, graphchain_optimize = optimizer

    # Replacement function 'goo'
    def goo(*args: int) -> int:
        # hash miss!
        return sum(args) + 1

    # Replacement function 'top'
    def top(argument: int, argument2: int) -> int:
        # hash miss!
        return argument - argument2

    moddata = {
        "goo1": (goo, {"goo1", "baz2", "top1"}, (-14, )),
        # "top1": (top, {"top1"}, (-14,)),
        "top1": (lambda *args: -14, {"top1"}, (-14, )),
        "v2": (1000, {"v2", "bar1", "boo1", "baz2", "top1"}, (-1012, ))
    }

    for (modkey, (taskobj, _affected_nodes, result)) in moddata.items():
        workdsk = dsk.copy()
        if callable(taskobj):
            workdsk[modkey] = (taskobj, *dsk[modkey][1:])
        else:
            workdsk[modkey] = taskobj

        newdsk = graphchain_optimize(workdsk, keys=["top1"])
        assert result == dask.get(newdsk, ["top1"])


def test_exec_only_nodes(
        dask_graph: dict,
        optimizer_exec_only_nodes: Tuple[str, Callable]) -> None:
    """Test skipping some tasks.

    Tests that execution-only nodes execute in the event that dependencies of
    their parent nodes (i.e. in the dask graph) get modified.
    """
    dsk = dask_graph
    cache_dir, graphchain_optimize = optimizer_exec_only_nodes

    # Cleanup temporary directory
    filelist = os.listdir(cache_dir)
    for entry in filelist:
        entrypath = os.path.join(cache_dir, entry)
        if os.path.isdir(entrypath):
            shutil.rmtree(entrypath, ignore_errors=True)
        else:
            os.remove(entrypath)
    filelist = os.listdir(cache_dir)
    assert not filelist

    # Run optimizer first time
    newdsk = graphchain_optimize(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])
    assert result == (-14, )

    # Modify function
    def goo(*args: int) -> int:
        # hash miss this!
        return sum(args) + 1

    dsk["goo1"] = (goo, *dsk["goo1"][1:])

    # Run optimizer a second time
    newdsk = graphchain_optimize(dsk, keys=["top1"])

    # Check the final result:
    # The output of node 'boo1' is needed at node 'baz2'
    # because 'goo1' was modified. A matching result indicates
    # that the boo1 node was executed, its dependencies loaded
    # which is the desired behaviour in such cases.
    result = dask.get(newdsk, ["top1"])
    assert result == (-14, )


def test_cache_deletion(
        dask_graph: dict,
        optimizer: Tuple[str, Callable]) -> None:
    """Test cache deletion.

    Tests the ability to obtain results in the event that cache files are
    deleted (in the even of a cache-miss, the exec-store wrapper should be
    re-run by the load-wrapper).
    """
    dsk = dask_graph
    cache_dir, graphchain_optimize = optimizer
    storage = fs.osfs.OSFS(cache_dir)

    # Cleanup first
    storage.removetree("/")

    # Run optimizer (first time)
    newdsk = graphchain_optimize(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])

    newdsk = graphchain_optimize(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])

    # Check the final result
    assert result == (-14, )


def test_identical_nodes(optimizer: Tuple[str, Callable]) -> None:
    """Small test for the presence of identical nodes."""
    cache_dir, graphchain_optimize = optimizer

    def foo(x: int) -> int:
        return x + 1

    def bar(*args: int) -> int:
        return sum(args)

    dsk = {"foo1": (foo, 1), "foo2": (foo, 1), "top1": (bar, "foo1", "foo2")}

    # First run
    newdsk = graphchain_optimize(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])
    assert result == (4, )

    # Second run
    newdsk = graphchain_optimize(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])
    assert result == (4, )
