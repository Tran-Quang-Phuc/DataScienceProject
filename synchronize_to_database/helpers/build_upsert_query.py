from typing import List, Iterable, Dict
from psycopg2 import connect, DatabaseError
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row


def get_postgres_connnection(
    host: str,
    database: str,
    user: str,
    password: str,
    port: str
):
    """
    Connect to postgres database and get the connection.
    :param host: host name of database instance.
    :param database: name of the database to connect to.
    :param user: user name.
    :param password: password for the user name.
    :param port: port to connect.
    :return: Database connection.
    """
    try:
        conn = connect(
            host=host, database=database,
            user=user, password=password,
            port=port
        )
    except (Exception, DatabaseError) as ex:
        print("Unable to connect to database !!")
        raise ex
    
    return conn


def build_upsert_query(
    cols: List[str],
    table_name: str,
    unique_key: List[str],
    cols_not_for_update: List[str] = None
) -> str:
    """
    Builds postgres upsert query using input arguments.
    Example : build_upsert_query(
        ['col1', 'col2', 'col3', 'col4'],
        "my_table",
        ['col1'],
        ['col2']
    ) ->
    INSERT INTO my_table (col1, col2, col3, col4) VALUES %s  
    ON CONFLICT (col1) DO UPDATE SET (col3, col4) = (EXCLUDED.col3, EXCLUDED.col4) ;
    :param cols: the postgres table columns required in the 
        insert part of the query.
    :param table_name: the postgres table name.
    :param unique_key: unique_key of the postgres table for checking 
        unique constraint violations.
    :param cols_not_for_update: columns in cols which are not required in
        the update part of upsert query.
    :return: Upsert query as per input arguments.
    """

    cols_str = ', '.join(cols)
    insert_query = """ INSERT INTO %s (%s) VALUES %%s """ % (
        table_name, cols_str
    )

    if cols_not_for_update is not None:
        cols_not_for_update.extend(unique_key)
    else:
        cols_not_for_update = [col for col in unique_key]
    
    unique_key_str = ', '.join(unique_key)

    update_cols = [col for col in cols if col not in cols_not_for_update]
    update_cols_str = ', '.join(update_cols)

    update_cols_with_excluded_markers = [f'EXCLUDED.{col}' for col in update_cols]
    update_cols_with_excluded_markers_str = ', '.join(update_cols_with_excluded_markers)

    on_conflict_clause = """ ON CONFLICT (%s) DO UPDATE SET (%s) = (%s) ;""" % (
        unique_key_str,
        update_cols_str,
        update_cols_with_excluded_markers_str
    )

    return insert_query + on_conflict_clause