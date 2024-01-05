from typing import Iterable
from pyspark.sql import Row
from synchronize_to_database.helpers.build_upsert_query import get_postgres_connnection
from synchronize_to_database.helpers.execute_values_with_err_handling import execute_values_with_err_handling


def batch_and_upsert(
    dataframe_partition: Iterable[Row],
    sql: str,
    database_credentials: dict,
    batch_size: int = 1000
):
    """
    Batch the input dataframe_partition as per batch_size and upsert
    to postgres using psycopg2 execute values.
    :param dataframe_partition: Pyspark DataFrame partition or any iterable.
    :param sql: query to insert/upsert the spark dataframe partition to postgres.
    :param database_credentials: postgres database credentials.
        Example: database_credentials = {
                host: <host>,
                database: <database>,
                user: <user>,
                password: <password>,
                port: <port>
            }
    :param batch_size: size of batch per round trip to database.
    :return: total records processed.
    """
    conn, cur = None, None
    counter = 0
    error_counter = 0
    batch, batch_list, final_error_msgs = [], [], []

    for record in dataframe_partition:
        counter += 1
        batch.append(record)

        if not conn:
            conn = get_postgres_connnection(**database_credentials)
            cur = conn.cursor()

        if counter % batch_size == 0:
            batch_list.append(batch)
            total_error_count, total_error_msgs = execute_values_with_err_handling(
                db_cur=cur,
                batch_list=batch_list,
                sql=sql
            )
            conn.commit()
            error_counter += total_error_count
            final_error_msgs.append(total_error_msgs)
            batch, batch_list = [], []

            # If entire batch gets rejected, stop processing further.
            if total_error_count == batch_size:
                break

    if batch:
        batch_list.append(batch)
        total_error_count, total_error_msgs = execute_values_with_err_handling(
            db_cur=cur,
            batch_list=batch_list,
            sql=sql
        )
        conn.commit()
        error_counter += total_error_count
        final_error_msgs.append(total_error_msgs)

    if cur:
        cur.close()
    if conn:
        conn.close()

    yield counter, error_counter, final_error_msgs

