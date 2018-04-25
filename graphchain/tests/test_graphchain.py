"""
Test module for the graphchain and funcutils modules.
Based on the 'pytest' test framework.
"""
import os
import shutil
from collections import Iterable
import pytest
import dask
from dask.optimization import get_dependencies
import fs
import fs.osfs
import fs_s3fs
from ..graphchain import gcoptimize
from ..funcutils import load_hashchain


@pytest.fixture(scope="function")
def dask_dag_generation():
    """
    Generates a dask compatible graph of the form,
    which will be used as a basis for the functional
    testing of the graphchain module:

		     O top(..)
                 ____|____
		/	  \
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
    """ # noqa
    # Functions
    def foo(argument):
        return argument

    def bar(argument):
        return argument + 2

    def baz(*args):
        return sum(args)

    def boo(*args):
        return len(args)+sum(args)

    def goo(*args):
        return sum(args) + 1

    def top(argument, argument2):
        return argument - argument2

    # Graph (for the function definitions above)
    dsk = {"v1": 1, "v2": 2, "v3": 3, "v4": 0,
           "v5": -1, "v6": -2, "d1": -3,
           "foo1": (foo, "v1"),
           "foo2": (foo, "v4"),
           "bar1": (bar, "v2"),
           "bar2": (bar, "v5"),
           "baz1": (baz, "v3"),
           "baz2": (baz, "boo1", "goo1"),
           "boo1": (boo, "foo1", "bar1", "baz1"),
           "goo1": (goo, "foo2", "bar2", "v6"),
           "top1": (top, "d1", "baz2")}
    return dsk


def test_dag(dask_dag_generation):
    """
    Tests that the dask DAG can be traversed correctly
    and that the actual result for the 'top1' key is correct.
    """
    dsk = dask_dag_generation
    result = dask.get(dsk, ["top1"])
    assert result == (-14,)


@pytest.fixture(scope="module")
def temporary_directory():
    """
    Creates the directory used for the graphchain tests.
    After the tests finish, it will be removed.
    """
    directory = os.path.abspath("__pytest_graphchain_cache__")
    if os.path.isdir(directory):
        shutil.rmtree(directory, ignore_errors=True)
    os.mkdir(directory, mode=0o777)
    yield directory
    shutil.rmtree(directory, ignore_errors=True)
    print(f"Cleanup of {directory} complete.")


@pytest.fixture(scope="function", params=[False, True])
def optimizer(temporary_directory, request):
    """
    Returns a parametrized version of the ``gcoptimize``
    function necessary to test caching and with and
    without LZ4 compression.
    """
    tmpdir = temporary_directory
    if request.param:
        filesdir = os.path.join(tmpdir, "compressed")
    else:
        filesdir = os.path.join(tmpdir, "uncompressed")

    def graphchain_opt_func(dsk,
                            keys=["top1"]):
        return gcoptimize(dsk,
                          keys=keys,
                          cachedir=filesdir,
                          compression=request.param)
    return graphchain_opt_func, request.param, filesdir


@pytest.fixture(scope="function")
def optimizer_exec_only_nodes(temporary_directory):
    """
    Returns a parametrized version of the ``gcoptimize``
    function necessary to test execution-only nodes
    within graphchain-optimized dask graphs.
    """
    tmpdir = temporary_directory
    filesdir = os.path.join(tmpdir, "compressed")

    def graphchain_opt_func(dsk, keys=["top1"]):
        return gcoptimize(dsk,
                          keys=keys,
                          cachedir=filesdir,
                          compression=False,
                          no_cache_keys=["boo1"])   # "boo1" is hardcoded
    return graphchain_opt_func, filesdir


@pytest.fixture(scope="module")
def temporary_s3_storage():
    """
    Creates the directory used for the graphchain tests
    using Amazon S3 storage. After the tests finish, the
    directory will be removed.
    """
    directory = "__pytest_graphchain_cache__"
    s3bucket = "graphchain-test-bucket"
    storage = fs_s3fs.S3FS(s3bucket)

    if storage.isdir(directory):
        storage.removetree(directory)
    storage.makedir(directory)
    yield directory, s3bucket
    storage.removetree(directory)
    storage.close()
    print(f"Cleanup of {directory} (on Amazon S3) complete.")


@pytest.fixture(scope="function", params=[False, True])
def optimizer_s3(temporary_s3_storage, request):
    """
    Returns a parametrized version of the ``gcoptimize``
    function necessary to test the support for Amazon S3
    storage.
    """
    tmpdir, s3bucket = temporary_s3_storage

    def graphchain_opt_func(dsk, keys=["top1"]):
        return gcoptimize(dsk,
                          keys=keys,
                          compression=request.param,
                          cachedir=tmpdir,
                          persistency="s3",
                          s3bucket=s3bucket)
    return graphchain_opt_func, tmpdir, s3bucket, request.param


def test_first_run(dask_dag_generation, optimizer):
    """
    Tests a first run of the graphchain optimization
    function ``gcoptimize``. It checks the final result,
    that that all function calls are wrapped - for
    execution and output storing, that the hashchain is
    created, that hashed outputs (the <hash>.pickle[.lz4] files)
    are generated and that the name of each file is a key
    in the hashchain.
    """
    dsk = dask_dag_generation
    fopt, compression, filesdir = optimizer

    if compression:
        data_ext = ".pickle.lz4"
    else:
        data_ext = ".pickle"
    hashchainfile = "graphchain.json"

    # Run optimizer
    newdsk = fopt(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14,)

    # Check that all functions have been wrapped
    for key, task in dsk.items():
        newtask = newdsk[key]
        assert newtask[0].__name__ == "exec_store_wrapper"
        if isinstance(task, Iterable):
            assert newtask[1:] == task[1:]
        else:
            assert not newtask[1:]

    # Check that the hash files are written and that each
    # filename can be found as a key in the hashchain
    # (the association of hash <-> DAG tasks is not tested)
    storage = fs.osfs.OSFS(filesdir)
    filelist = storage.listdir("/")
    filelist_cache = storage.listdir("cache")
    nfiles = sum(map(lambda x: x.endswith(data_ext), filelist_cache))

    assert hashchainfile in filelist
    assert nfiles == len(dsk)

    hashchain = load_hashchain(storage, compression=compression)
    storage.close()

    for filename in filelist_cache:
        if len(filename) == 43:
            assert filename[-11:] == ".pickle.lz4"
        elif len(filename) == 39:
            assert filename[-7:] == ".pickle"
        else:  # there should be no other files in the directory
            assert False
        assert str.split(filename, ".")[0] in hashchain.keys()


def DISABLED_test_single_run_s3(dask_dag_generation, optimizer_s3):
    """
    Tests a single run of the graphchain optimization
    function ``gcoptimize`` using Amazon S3 as a
    persistency layer. It checks the final result,
    that that all function calls are wrapped - for
    execution and output storing, that the hashchain is
    created, that hashed outputs (the <hash>.pickle[.lz4] files)
    are generated and that the name of each file is a key
    in the hashchain.
    """
    dsk = dask_dag_generation
    fopt, filesdir, s3bucket, compression = optimizer_s3

    # Run optimizer
    newdsk = fopt(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14,)

    if compression:
        data_ext = ".pickle.lz4"
    else:
        data_ext = ".pickle"
    hashchainfile = "graphchain.json"

    # Check that all functions have been wrapped
    for key, task in dsk.items():
        newtask = newdsk[key]
        assert newtask[0].__name__ == "exec_store_wrapper"
        if isinstance(task, Iterable):
            assert newtask[1:] == task[1:]
        else:
            assert not newtask[1:]

    # Check that the hash files are written and that each
    # filename can be found as a key in the hashchain
    # (the association of hash <-> DAG tasks is not tested)
    storage = fs_s3fs.S3FS(s3bucket, filesdir)
    filelist = storage.listdir("/")
    filelist_cache = storage.listdir("/cache")
    nfiles = sum(map(lambda x: x.endswith(data_ext), filelist_cache))

    assert hashchainfile in filelist
    assert nfiles == len(dsk)

    hashchain = load_hashchain(storage, compression=compression)

    for filename in filelist_cache:
        if len(filename) == 43:
            assert filename[-11:] == ".pickle.lz4"
        elif len(filename) == 39:
            assert filename[-7:] == ".pickle"
        else:  # there should be no other files in the directory
            assert False
        assert str.split(filename, ".")[0] in hashchain.keys()

    # Cleanup (the main directory will be removed by the
    # temporary directory fixture)
    storage.removetree('cache')
    storage.remove('graphchain.json')
    assert not storage.listdir('/')


def test_second_run(dask_dag_generation, optimizer):
    """
    Tests a second run of the graphchain optimization function `gcoptimize`.
    It checks the final result, that that all function calls are
    wrapped - for loading and the the result key has no dependencies.
    """
    dsk = dask_dag_generation
    fopt, _, _ = optimizer

    # Run optimizer
    newdsk = fopt(dsk, keys=["top1"])

    # Check the final result
    result = dask.get(newdsk, ["top1"])
    assert result == (-14,)

    # Check that the functions are wrapped for loading
    for key in dsk.keys():
        newtask = newdsk[key]
        assert isinstance(newtask, tuple)
        assert newtask[0].__name__ == "loading_wrapper"
        assert len(newtask) == 1  # only the loading wrapper


def test_node_changes(dask_dag_generation, optimizer):
    """
    Tests the functionality of the graphchain in the event of changes
    in the structure of the graph, namely by altering the functions/constants
    associated to the tasks. After optimization, the afected nodes should
    be wrapped in a storeand execution wrapper and their dependency lists
    should not be empty.
    """
    dsk = dask_dag_generation
    fopt, _, _ = optimizer

    # Replacement function 'goo'
    def goo(*args):
        # hash miss!
        return sum(args) + 1

    # Replacement function 'top'
    def top(argument, argument2):
        # hash miss!
        return argument - argument2

    moddata = {"goo1": (goo, {"goo1", "baz2", "top1"}, (-14,)),
               # "top1": (top, {"top1"}, (-14,)),
               "top1": (lambda *args: -14, {"top1"}, (-14,)),
               "v2": (1000, {"v2", "bar1", "boo1", "baz2", "top1"}, (-1012,))
               }

    for (modkey, (taskobj, affected_nodes, result)) in moddata.items():
        workdsk = dsk.copy()
        if callable(taskobj):
            workdsk[modkey] = (taskobj, *dsk[modkey][1:])
        else:
            workdsk[modkey] = taskobj

        newdsk = fopt(workdsk, keys=["top1"])
        assert result == dask.get(newdsk, ["top1"])

        for key, newtask in newdsk.items():
            if callable(taskobj):
                if key in affected_nodes:
                    assert newtask[0].__name__ == "exec_store_wrapper"
                    assert get_dependencies(newdsk, key)
                else:
                    assert newtask[0].__name__ == "loading_wrapper"
                    assert not get_dependencies(newdsk, key)
            else:
                if key in affected_nodes and key == modkey:
                    assert newtask[0].__name__ == "exec_store_wrapper"
                    assert not get_dependencies(newdsk, key)
                elif key in affected_nodes:
                    assert newtask[0].__name__ == "exec_store_wrapper"
                    assert get_dependencies(newdsk, key)
                else:
                    assert newtask[0].__name__ == "loading_wrapper"
                    assert not get_dependencies(newdsk, key)


def test_exec_only_nodes(dask_dag_generation, optimizer_exec_only_nodes):
    """
    Tests that execution-only nodes execute in the event
    that dependencies of their parent nodes (i.e. in the
    dask graph) get modified.
    """
    dsk = dask_dag_generation
    fopt, filesdir = optimizer_exec_only_nodes

    # Cleanup temporary directory
    filelist = os.listdir(filesdir)
    for entry in filelist:
        entrypath = os.path.join(filesdir, entry)
        if os.path.isdir(entrypath):
            shutil.rmtree(entrypath, ignore_errors=True)
        else:
            os.remove(entrypath)
    filelist = os.listdir(filesdir)
    assert not filelist

    # Run optimizer first time
    newdsk = fopt(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])
    assert result == (-14,)

    # Modify function
    def goo(*args):
        # hash miss this!
        return sum(args) + 1

    dsk["goo1"] = (goo, *dsk["goo1"][1:])

    # Run optimizer a second time
    newdsk = fopt(dsk, keys=["top1"])

    # Check the final result:
    # The output of node 'boo1' is needed at node 'baz2'
    # because 'goo1' was modified. A matching result indicates
    # that the boo1 node was executed, its dependencies loaded
    # which is the desired behaviour in such cases.
    result = dask.get(newdsk, ["top1"])
    assert result == (-14,)


def test_cache_deletion(dask_dag_generation, optimizer):
    """
    Tests the ability to obtain results in the event that
    cache files are deleted (in the even of a cache-miss,
    the exec-store wrapper should be re-run by the
    load-wrapper).
    """
    dsk = dask_dag_generation
    fopt, compression, filesdir = optimizer
    storage = fs.osfs.OSFS(filesdir)

    # Cleanup first
    storage.removetree("/")

    # Run optimizer (first time)
    newdsk = fopt(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])

    # Remove all of the cache
    filelist_cache = storage.listdir("cache")
    for _file in filelist_cache:
        storage.remove(fs.path.join("cache", _file))

    newdsk = fopt(dsk, keys=["top1"])
    result = dask.get(newdsk, ["top1"])

    # Check the final result
    assert result == (-14,)
