import configparser
import psycopg2
import sys
from sql_queries import (
    create_table_queries,
    drop_table_queries,
    create_schema_queries,
    drop_schema_queries,
)


def drop_schemas(cur, conn):
    for query in drop_schema_queries:
        cur.execute(query)
        conn.commit()
    print("schemas dropped")


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("tables dropped")


def create_schemas(cur, conn):
    for query in create_schema_queries:
        cur.execute(query)
        conn.commit()
    print("schemas created")


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("tables created")


def main():
    config = configparser.ConfigParser()
    config.read("../dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config["DB"].values())
    )
    cur = conn.cursor()
    drop_schemas(cur, conn)
    drop_tables(cur, conn)
    create_schemas(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
