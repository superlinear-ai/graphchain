"""Test module for the dask HighLevelGraphs."""

from functools import partial
from typing import Any, Dict, cast

import dask
import fs.base
import pandas as pd
import pytest
from dask.highlevelgraph import HighLevelGraph
from fs.memoryfs import MemoryFS

from graphchain.core import optimize


@pytest.fixture()
def dask_high_level_graph() -> HighLevelGraph:
    """Generate an example dask HighLevelGraph."""

    @dask.delayed(pure=True)
    def create_dataframe(num_rows: int, num_cols: int) -> pd.DataFrame:
        return pd.DataFrame(data=[range(num_cols)] * num_rows)

    @dask.delayed(pure=True)
    def create_dataframe2(num_rows: int, num_cols: int) -> pd.DataFrame:
        return pd.DataFrame(data=[range(num_cols)] * num_rows)

    @dask.delayed(pure=True)
    def complicated_computation(df: pd.DataFrame, num_quantiles: int) -> pd.DataFrame:
        return df.quantile(q=[i / num_quantiles for i in range(num_quantiles)])

    @dask.delayed(pure=True)
    def summarise_dataframes(*dfs: pd.DataFrame) -> float:
        return sum(cast(pd.Series[float], df.sum()).sum() for df in dfs)

    df_a = create_dataframe(1000, 1000)
    df_b = create_dataframe2(1000, 1000)
    df_c = complicated_computation(df_a, 2048)
    df_d = complicated_computation(df_b, 2048)
    result: HighLevelGraph = summarise_dataframes(df_c, df_d)
    return result


def test_high_level_dag(dask_high_level_graph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct."""
    with dask.config.set(scheduler="sync"):
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
    assert result == 2045952000.0


def test_high_level_graph(dask_high_level_graph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct."""
    dask.config.set({"cache_latency": 0, "cache_throughput": float("inf")})
    with dask.config.set(scheduler="sync", delayed_optimize=optimize):
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0


def test_high_level_graph_parallel(dask_high_level_graph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct when using parallel scheduler."""
    dask.config.set({"cache_latency": 0, "cache_throughput": float("inf")})
    with dask.config.set(scheduler="processes", delayed_optimize=optimize):
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0


def test_custom_serde(dask_high_level_graph: HighLevelGraph) -> None:
    """Test that we can use a custom serializer/deserializer."""
    custom_cache: Dict[str, Any] = {}

    def custom_serialize(obj: Any, fs: fs.base.FS, key: str) -> None:
        # Write the key itself to the filesystem.
        with fs.open(f"{key}.dat", "wb") as fid:
            fid.write(key)
        # Store the actual result in an in-memory cache.
        custom_cache[key] = result

    def custom_deserialize(fs: fs.base.FS, key: str) -> Any:
        # Verify that we have written the key to the filesystem.
        with fs.open(f"{key}.dat", "rb") as fid:
            assert key == fid.read()
        # Get the result corresponding to that key.
        return custom_cache[key]

    # Use a custom location so that we don't corrupt the default cache.
    custom_optimize = partial(
        optimize,
        location=MemoryFS(),
        serialize=custom_serialize,
        deserialize=custom_deserialize,
    )

    # Ensure everything gets cached.
    dask.config.set({"cache_latency": 0, "cache_throughput": float("inf")})

    with dask.config.set(scheduler="sync", delayed_optimize=custom_optimize):
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0
        result = dask_high_level_graph.compute()  # type: ignore[attr-defined]
        assert result == 2045952000.0
