import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes the COPY queries in sql_queries.py
    and copies JSON data from S3 into Amazon Redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes the INSERT queries in sql_queries.py
    and inserts the data that was copied into a newly created schema
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # load configuration for Amazon Redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load tables into Redshift
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()