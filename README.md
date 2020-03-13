# Data Model and ETL for Song Play Events using Apache Cassandra

## Objectives
Song play events data is given as a csv file. Aim of this project is to support 3 different queries expected on this data. One model / table per query was created as no query was a more specified version of another query. An ETL was also created along with a simple test script. 

## Files
Project has the following files:
- `etl.py` contains the ETL. Execute `python etl.py -h` to see usage instructions.
- `queries.py` contains the data model and other queries needed by ETL.
- `test.py` runs 1 test query per table and prints the results.

*Note: the scripts were tested on Python 3.*
