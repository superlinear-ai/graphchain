[![CircleCI](https://img.shields.io/circleci/token/39b1cfd1096f95ab3c6aeb839d86763ea2a261aa/project/github/radix-ai/graphchain/master.svg)](https://circleci.com/gh/radix-ai/graphchain/tree/master) [![License](https://img.shields.io/github/license/mashape/apistatus.svg)](https://choosealicense.com/licenses/mit/) [![PyPI](https://img.shields.io/pypi/v/graphchain.svg)](https://pypi.python.org/pypi/graphchain/) [![Documentation](http://readthedocs.org/projects/graphchain/badge/?version=latest)](http://graphchain.readthedocs.io/)

# Graphchain

## What is graphchain?

Graphchain is like [joblib.Memory](https://joblib.readthedocs.io/en/latest/memory.html#memory) for dask graphs. [Dask graph computations](http://dask.pydata.org/en/latest/spec.html) are cached to a local or remote location of your choice, specified by a [PyFilesystem FS URL](https://docs.pyfilesystem.org/en/latest/openers.html).

When you change your dask graph (by changing a computation's implementation or its inputs), graphchain will take care to only recompute the minimum number of computations necessary to fetch the result. This allows you to iterate quickly over your graph without spending time on recomputing previously computed keys.

<p align="center">
    <img src="https://imgs.xkcd.com/comics/is_it_worth_the_time_2x.png" width="400" /><br />
    <span>Source: <a href="https://xkcd.com/1205/">xkcd.com/1205/</a></span>
</p>

The main difference between graphchain and joblib.Memory is that in graphchain a computation's materialised inputs are _not_ serialised and hashed (which can be very expensive when the inputs are large objects such as pandas DataFrames). Instead, a chain of hashes (hence the name graphchain) of the computation object and its dependencies (which are also computation objects) is used to identify the cache file.

Additionally, the result of a computation is only cached if it is estimated that loading that computation from cache will save time compared to simply computing the computation. The decision on whether to cache depends on the characteristics of the cache location, which are different when caching to the local filesystem compared to caching to S3 for example.

## Usage by example

### Basic usage

Install graphchain with pip to get started:

```bash
pip install graphchain
```

To demonstrate how graphchain can save you time, let's first create a simple dask graph that (1) creates a few pandas DataFrames, (2) runs a relatively heavy operation on these DataFrames, and (3) summarises the results.

```python
import dask
import graphchain
import pandas as pd

def create_dataframe(num_rows, num_cols):
    print('Creating DataFrame...')
    return pd.DataFrame(data=[range(num_cols)]*num_rows)

def complicated_computation(df, num_quantiles):
    print('Running complicated computation on DataFrame...')
    return df.quantile(q=[i / num_quantiles for i in range(num_quantiles)])

def summarise_dataframes(*dfs):
    print('Summing DataFrames...')
    return sum(df.sum().sum() for df in dfs)

dsk = {
    'df_a': (create_dataframe, 10_000, 1000),
    'df_b': (create_dataframe, 10_000, 1000),
    'df_c': (complicated_computation, 'df_a', 2048),
    'df_d': (complicated_computation, 'df_b', 2048),
    'result': (summarise_dataframes, 'df_c', 'df_d')
}
```

Using `dask.get` to fetch the `'result'` key takes about 6 seconds:

```python
>>> %time dask.get(dsk, 'result')

Creating DataFrame...
Running complicated computation on DataFrame...
Creating DataFrame...
Running complicated computation on DataFrame...
Summing DataFrames...

CPU times: user 7.39 s, sys: 686 ms, total: 8.08 s
Wall time: 6.19 s
```

On the other hand, using `graphchain.get` for the first time to fetch `'result'` takes only 4 seconds:

```python
>>> %time graphchain.get(dsk, 'result')

Creating DataFrame...
Running complicated computation on DataFrame...
Summing DataFrames...

CPU times: user 4.7 s, sys: 519 ms, total: 5.22 s
Wall time: 4.04 s
```

The reason `graphchain.get` is faster than `dask.get` is because it can load `df_b` and `df_d` from cache after `df_a` and `df_c` have been computed and cached. Note that graphchain will only cache the result of a computation if loading that computation from cache is estimated to be faster than simply running the computation.

Running `graphchain.get` a second time to fetch `'result'` will be almost instant since this time the result itself is also available from cache:

```python
>>> %time graphchain.get(dsk, 'result')

CPU times: user 4.79 ms, sys: 1.79 ms, total: 6.58 ms
Wall time: 5.34 ms
```

Now let's say we want to change how the result is summarised from a sum to an average:

```python
def summarise_dataframes(*dfs):
    print('Averaging DataFrames...')
    return sum(df.mean().mean() for df in dfs) / len(dfs)
```

If we then ask graphchain to fetch `'result'`, it will detect that only `summarise_dataframes` has changed and therefore only recompute this function with inputs loaded from cache:

```python
>>> %time graphchain.get(dsk, 'result')

Averaging DataFrames...

CPU times: user 123 ms, sys: 37.2 ms, total: 160 ms
Wall time: 86.6 ms
```

### Storing the graphchain cache remotely

Graphchain's cache is by default `./__graphchain_cache__`, but you can ask graphchain to use a cache at any [PyFilesystem FS URL](https://docs.pyfilesystem.org/en/latest/openers.html) such as `s3://mybucket/__graphchain_cache__`:

```python
graphchain.get(dsk, 'result', location='s3://mybucket/__graphchain_cache__')
```

### Excluding keys from being cached

In some cases you may not want a key to be cached. To avoid writing certain keys to the graphchain cache, you can use the `skip_keys` argument:

```python
graphchain.get(dsk, 'result', skip_keys=['result'])
```

### Using graphchain with dask.delayed

Alternatively, you can use graphchain together with dask.delayed for easier dask graph creation:

```python
@dask.delayed
def create_dataframe(num_rows, num_cols):
    print('Creating DataFrame...')
    return pd.DataFrame(data=[range(num_cols)]*num_rows)

@dask.delayed
def complicated_computation(df, num_quantiles):
    print('Running complicated computation on DataFrame...')
    return df.quantile(q=[i / num_quantiles for i in range(num_quantiles)])

@dask.delayed
def summarise_dataframes(*dfs):
    print('Summing DataFrames...')
    return sum(df.sum().sum() for df in dfs)

df_a = create_dataframe(num_rows=50_000, num_cols=500, seed=42)
df_b = create_dataframe(num_rows=50_000, num_cols=500, seed=42)
df_c = complicated_computation(df_a, window=3)
df_d = complicated_computation(df_b, window=3)
result = summarise_dataframes(df_c, df_d)
```

After which you can compute `result` by setting the `delayed_optimize` method to `graphchain.optimize`:

```python
with dask.config.set(scheduler='sync', delayed_optimize=graphchain.optimize):
    result.compute(location='s3://mybucket/__graphchain_cache__')
```

## Developed by radix.ai

At [radix.ai](https://radix.ai/), we invent, design and develop AI-powered software.

Here are some examples of what we do with Machine Learning, the technology behind AI:
- Help job seekers find a job. On the [Belgian Public Employment Service website](https://www.vdab.be/), we serve job recommendations based on your CV.
- Help hospitals save time. We extract diagnoses from patient discharge letters.
- Help publishers calculate their impact, by detecting copycat articles.

You can follow our adventures on [medium](https://medium.com/radix-ai-blog).
