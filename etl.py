import argparse
import csv
import textwrap
from cassandra.cluster import Cluster
from queries import *


def load_data(session, insert_query, extract_fn, path_to_csv, encoding="utf8",
              contains_header=True):
    """ Load data from csv file into a table.

    Args:
        session cassandra session
        insert_query query for inserting one datapoint at a time
        extract_fn function to transform line read from csv to data
            tuple for insertion
        path_to_csv path to csv file with data
        encoding string encoding to use while reading csv
        contains_header if csv doesn't contains headers, set it to False
    Returns:
        None
    Preconditions:
        session keyspace must be set to match table in insert query
    """
    with open(path_to_csv, encoding=encoding) as f:

        # obtain csv reader
        csvreader = csv.reader(f)

        # skip header
        if contains_header:
            next(csvreader)

        # inserting all records into the table
        for line in csvreader:
            session.execute(insert_query, extract_fn(line))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent("\
            Load song play events data into Cassandra. The data model is \n\
            optimized for queries provided.")
    )

    # get path to csv file
    parser.add_argument(
        "--csv",
        required=True,
        metavar="PATH",
        help="path to .csv file"
    )

    # get cluster endpoint
    parser.add_argument(
        "--host",
        help="cluster ip / endpoint"
    )

    args = parser.parse_args()

    # establish a session with local cluster
    cluster = Cluster([args.host if args.host else "127.0.0.1"])
    session = cluster.connect()

    # set session to events keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS events WITH
        replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("events")

    # load data pertaining to all queries
    for create_table, insert_query, extract_fn in zip(tables, inserts,
                                                      data_extractions):
        session.execute(create_table)
        load_data(session, insert_query, extract_fn, args.csv)

    # close connection to the cluster
    session.shutdown()
    cluster.shutdown()
