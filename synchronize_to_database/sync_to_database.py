from config import DBConfig
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import *
from synchronize_to_database.upsert_to_db import upsert_to_db


def sync_to_db(
    warehouse_table: str,
    db_table: str,
):
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    warehosue_df = spark.read.format("delta").load(warehouse_table)
    to_sync_df = warehosue_df.filter(F.col("updated_at") == F.current_date())
    upsert_to_db(to_sync_df, db_table)

    