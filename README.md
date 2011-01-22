# Simple Haskell MapReduce Framework


## Objective
To establish a library that is dirt-easy to use in processing fairly large
amounts of data. It is meant to process data that is too much to handle for one
laptop through traditional means, but is not necesarrily meant to scale to
google levels.

This package is for the practical-minded Haskell data hacker.


## v0.1 Concept & Design Choices

* Input data is flat-file
* Reduces are idempotent, Finalizer does the final data transformation
* Unified feeder + data-mapper
* Data-mapper reduces set after mapping for simplicity & space efficiency
* Multiple mappers are possible if input data can be partitioned
* Multiple finalizers are possible
* Redis backend
	  - mr-jobs db to store job info
	  - each job gets 3 dbs:
	  	  - mapped data
	  	  - locked data - atomic move during reduce
	  	  - finalized data
* Finalized data extracted into a CSV file

