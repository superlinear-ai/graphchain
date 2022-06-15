"""Test module for dask DataFrames."""

import os
from functools import partial
from typing import Any

import dask
import dask.dataframe
import fs.base
import fs.osfs
import joblib
import pytest
from dask.highlevelgraph import HighLevelGraph

from graphchain.core import optimize


@pytest.fixture()
def dask_dataframe_graph() -> HighLevelGraph:
    """Generate an example dask DataFrame graph."""

    @dask.delayed(pure=True)
    def create_dataframe() -> dask.dataframe.DataFrame:
        df: dask.dataframe.DataFrame = dask.datasets.timeseries(seed=42)
        return df

    @dask.delayed(pure=True)
    def summarise_dataframe(df: dask.dataframe.DataFrame) -> float:
        value: float = df["x"].sum().compute() + df["y"].sum().compute()
        return value

    df = create_dataframe()
    result: HighLevelGraph = summarise_dataframe(df)
    return result


def test_dask_dataframe_graph(dask_dataframe_graph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct."""
    dask.config.set({"cache_latency": 0, "cache_throughput": float("inf")})
    with dask.config.set(scheduler="sync", delayed_optimize=optimize):
        result = dask_dataframe_graph.compute()  # type: ignore[attr-defined]
        assert result == 856.0466289487188
        result = dask_dataframe_graph.compute()  # type: ignore[attr-defined]
        assert result == 856.0466289487188


def test_custom_serde(dask_dataframe_graph: HighLevelGraph) -> None:
    """Test that we can use a custom serializer/deserializer."""

    def custom_serialize(obj: Any, fs: fs.osfs.OSFS, key: str) -> None:
        if isinstance(obj, dask.dataframe.DataFrame):
            obj.to_parquet(os.path.join(fs.root_path, key))
        else:
            with fs.open(f"{key}.joblib", "wb") as fid:
                joblib.dump(obj, fid)

    def custom_deserialize(fs: fs.osfs.OSFS, key: str) -> Any:
        if fs.exists(f"{key}.joblib"):
            with fs.open(f"{key}.joblib", "rb") as fid:
                return joblib.load(fid)
        else:
            return dask.dataframe.read_parquet(os.path.join(fs.root_path, key))

    custom_optimize = partial(
        optimize,
        location="__graphchain_cache__/parquet/",
        serialize=custom_serialize,
        deserialize=custom_deserialize,
    )

    # Ensure everything gets cached.
    dask.config.set({"cache_latency": 0, "cache_throughput": float("inf")})

    with dask.config.set(scheduler="sync", delayed_optimize=custom_optimize):
        result = dask_dataframe_graph.compute()  # type: ignore[attr-defined]
        assert result == 856.0466289487188
        result = dask_dataframe_graph.compute()  # type: ignore[attr-defined]
        assert result == 856.0466289487188
