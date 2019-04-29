"""Test module for the dask HighLevelGraphs."""
import dask
import pandas as pd
import pytest
from dask.highlevelgraph import HighLevelGraph

from ..core import optimize


@pytest.fixture(scope="function")  # type: ignore
def dask_highlevelgraph() -> HighLevelGraph:
    """Generate an example dask HighLevelGraph."""
    @dask.delayed(pure=True)  # type: ignore
    def create_dataframe(num_rows: int, num_cols: int) -> pd.DataFrame:
        print('Creating DataFrame...')
        return pd.DataFrame(data=[range(num_cols)] * num_rows)

    @dask.delayed(pure=True)  # type: ignore
    def create_dataframe2(num_rows: int, num_cols: int) -> pd.DataFrame:
        print('Creating DataFrame...')
        return pd.DataFrame(data=[range(num_cols)] * num_rows)

    @dask.delayed(pure=True)  # type: ignore
    def complicated_computation(df: pd.DataFrame, num_quantiles: int) \
            -> pd.DataFrame:
        print('Running complicated computation on DataFrame...')
        return df.quantile(q=[i / num_quantiles for i in range(num_quantiles)])

    @dask.delayed(pure=True)  # type: ignore
    def summarise_dataframes(*dfs: pd.DataFrame) -> float:
        print('Summing DataFrames...')
        return sum(df.sum().sum() for df in dfs)

    df_a = create_dataframe(1000, 1000)
    df_b = create_dataframe2(1000, 1000)
    df_c = complicated_computation(df_a, 2048)
    df_d = complicated_computation(df_b, 2048)
    result = summarise_dataframes(df_c, df_d)
    return result


def test_highleveldag(dask_highlevelgraph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct."""
    with dask.config.set(scheduler='sync'):
        result = dask_highlevelgraph.compute()
    assert result == 2045952000.0


def test_highlevelgraph(dask_highlevelgraph: HighLevelGraph) -> None:
    """Test that the graph can be traversed and its result is correct."""
    with dask.config.set(scheduler='sync', delayed_optimize=optimize):
        result = dask_highlevelgraph.compute()
    assert result == 2045952000.0
