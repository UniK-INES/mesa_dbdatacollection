# mesa_dbdatacollection
DB data collector for the mesa agent-based modelling framework

The DB data collector extends the mesa DataCollector, initialises tables as required and writes collected data to the configured database instead of CSV files
Furthermore, it introduces a RunID object to associate any table row with a distinct object that describes the current simulation run (and which can be associated with according parameter settings).

## Requirements

DB data collection requires these python packages:

* mesa
* pysqlite
* sqlalchemy

## Installation

    pip install git+https://github.com/tpike3/bilateralshapley.git
    
## Configuration


## Example


## Implementation

## Performing tests

    pytest
    pytest -v -k 'not runInfo' tests/test_db_inserts.py

## Performing benchmarks

    pytest --benchmark-min-rounds=3 --benchmark-warmup=off

## Performing profiling

    python -m cProfile -o profile01.out example/runheadless_cprofile.py
    snakeviz profile01.out
