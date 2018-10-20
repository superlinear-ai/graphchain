"""Setup module for graphchain."""

from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='graphchain',
    version='1.0.0',
    description='An efficient cache for the execution of dask graphs',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/radix-ai/graphchain',
    author='radix.ai',
    author_email='developers@radix.ai',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='dask graph cache distributed',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['cloudpickle', 'dask', 'fs-s3fs', 'joblib', 'lz4'],
    project_urls={
        'Bug Reports': 'https://github.com/radix-ai/graphchain/issues',
        'Source': 'https://github.com/radix-ai/graphchain',
    },
)
