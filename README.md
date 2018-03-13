# graphchain

*Graphchain is an hash-chain based optimizer for dask. It can be employed
to optimize dask.delayed task execution graphs in order to improve the 
overall speed. It makes extensive use of caching.*

##TODOs
 - improved unit testing
 - re-work hashing mechanism
 - handle atypical dask.delayed objects i.e. delayed constants
 - improve diagnostics mechanism related to hash changes
