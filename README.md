# Simple Haskell MapReduce Framework


## Objective
To establish a library that is dirt-easy to use in processing fairly large
amounts of data. It is meant for datasets that are too much to handle for one
laptop through traditional means, but is not necesarrily meant to scale to
super-massive levels.

The main benefits are:

* Work directly with flat files so we can get around loading data into DBs.
* Haskell-only so we can easily automate input feeding through several
  sources/files.
* Keeps it simple so the average Haskell hacker can ramp up operations farily
  easily.

This package is for the practical-minded Haskell data hacker.


## v0.1 Concept & Design Choices

* Input data is flat-file (CSV-like)
* Reduces are idempotent, Finalizer does the final data transformation
* Unified feeder + data-mapper (split input into multiple files for multiple
  mappers)
* Unified reducer and finalizer
* Simple locking to enable multiple reducers
* Base everything on Redis for now; worry about multiple backends later
* Keep everything simple and tightly coupled to test out some of the ideas
  before blowing into polymorphic hell
* Redis backend
	  - mr-jobs db to store job info
	  - each job gets 3 dbs:
	  	  - mapped data
	  	  - locked data - atomic move during reduce
	  	  - finalized data
* Finalized data extracted into a CSV file

