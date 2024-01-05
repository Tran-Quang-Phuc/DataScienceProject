from typing import List, Iterable, Dict, Tuple
from contextlib import contextmanager
from psycopg2 import connect, DatabaseError, Error
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row


@contextmanager
def savepoint(
    sp_cur,
    sp_name,
    func,
    *args,
    **kwargs
) -> tuple:
    """
    A context manager to set savepoint, execute a database action
    and rollback to the savepoint in the event of an exception or
    yield the output and release the savepoint.
    :param sp_cur: psycopg2 cursor.
    :param sp_name: savepoint name.
    :param func: function to execute.
    :param args: any function arguments.
    :param kwargs: any function keyword arguments.
    :return: return tuple of function output and None or
    None and error as applicable.
    """

    try:
        sp_cur.execute(f"SAVEPOINT {sp_name};")
        output = func(*args, **kwargs)
    except (Exception, Error) as error:
        sp_cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
        yield None, error
    else:
        try:
            yield output, None
        finally:
            sp_cur.execute(f"RELEASE SAVEPOINT {sp_name}")


def execute_values_with_err_handling(
    db_cur,
    batch_list: List[List[Row]],
    sql: str
) -> Tuple[int, List[str]]:
    """
    Execute a database action with error handling.
    :param db_cur: psycopg2 cursor.
    :param batch_list: List of batches to load.
    :param sql: query to execute.
    :return: total error count and list of error messages.
    """
    total_error_count = 0
    total_error_msgs = []

    while batch_list:
        batch = batch_list.pop()

        with savepoint(
            db_cur, 'my_sp',
            execute_values, cur=db_cur, sql=sql,
            argslist=batch, page_size=len(batch)
        ) as (output, error):
            if error:
                split_batches = batch_error_handler(batch=batch)
            
                if split_batches:
                    batch_list.extend(split_batches)
                else:
                    total_error_count += 1
                    total_error_msgs.append(str(error))
    
    return total_error_count, total_error_msgs


def batch_error_handler(batch: List[Row]) -> List[List[Row]] or None:
    """
    Split the rejected batch into two equal halves and return the same.
    If however, the batch has only one record, return None to indicate
    that it is an error record.
    :param batch: Rejected batch.
    :return: List of split batches or None as applicable.
    """
    batch_size = len(batch)

    if batch_size == 1:
        return None
    
    chunk_size = batch_size // 2
    split_batches = [batch[i:i + chunk_size] for i in range(0, batch_size, chunk_size)]

    return split_batches

