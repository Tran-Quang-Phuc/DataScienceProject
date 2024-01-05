import typer
from typing import List, Dict
from utils.config_spark_delta import config_spark_delta
from synchronize_to_database.helpers.batch_and_upsert import batch_and_upsert
from synchronize_to_database.helpers.build_upsert_query import build_upsert_query


def upsert_spark_df_to_postgres(
    warehouse: str,
    table_name: str,
    table_unique_key: List[str],
    database_credentials: Dict[str, str],
    batch_size: int = 1000,
    parallelism: int = 1
):
    """
    Upsert a spark DataFrame into a postgres table.
    Note: If the target table lacks any unique index, data will be appended through
    INSERTS as UPSERTS in postgres require a unique constraint to be present in the table.
    :param dataframe_to_upsert: spark DataFrame to upsert to postgres.
    :param table_name: postgres table name to upsert.
    :param table_unique_key: postgres table primary key.
    :param database_credentials: database credentials.
    :param batch_size: desired batch size for upsert.
    :param parallelism: No. of parallel connections to postgres database.
    :return:None
    """
    spark = config_spark_delta()
    dataframe_to_upsert = spark.read.format("delta").load(warehouse)
    upsert_query = build_upsert_query(
        cols=dataframe_to_upsert.schema.names,
        table_name=table_name, unique_key=table_unique_key
    )
    upsert_stats = dataframe_to_upsert.coalesce(parallelism).rdd.mapPartitions(
        lambda dataframe_partition: batch_and_upsert(
            dataframe_partition=dataframe_partition,
            sql=upsert_query,
            database_credentials=database_credentials,
            batch_size=batch_size
        )
    )

    total_recs_loaded = 0
    total_recs_rejects = 0
    error_msgs = []

    for counter, error_counter, final_error_msgs in upsert_stats.collect():
        total_recs_loaded += counter
        total_recs_rejects += error_counter
        error_msgs.extend(final_error_msgs)

    print("")
    print("#################################################")
    print(f" Total records loaded - {total_recs_loaded}")
    print(f" Total records rejected - {total_recs_rejects}")
    print("#################################################")
    print("")

    for err_msg in error_msgs:
        print(" Started Printing Error Messages ....")
        print(err_msg)
        print(" Completed Printing Error Messages ....")