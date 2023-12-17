from typing import NamedTuple, Optional
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
from utils.merge_schema import merge_schema


class ColumnInfo(NamedTuple):
    name: str
    alias: Optional[str]


def upsert_to_warehouse(
    spark: SparkSession,
    job_df: DataFrame,
    warehouse_table: str,
    columns: list[ColumnInfo]
):
    cols = []
    for col in columns:
        alias = col.alias or col.name
        cols.append(F.col(col.name).alias(alias))
    job_df = job_df.select(*cols)
    job_df = job_df.withColumn("created_at", F.current_date()).withColumn("updated_at", F.current_date())

    if DeltaTable.isDeltaTable(spark, warehouse_table):
        warehouse = DeltaTable.forPath(spark, warehouse_table)
        warehouse, job_df = merge_schema(spark, warehouse, job_df)
        warehouse_df = warehouse.toDF()

        update_cols = {}
        for column in columns:
            col = column.alias or column.name
            update_cols[col] = F.when(job_df[col].isNotNull(), job_df[col]).otherwise(warehouse_df[col])
        update_cols["updated_at"] = job_df["updated_at"]
        
        # to filter 2 job is the same
        merge_conditions = """
            warehouse.id = job.id 
            AND warehouse.company_name = job.company_name
            AND warehouse.title = job.title
        """

        warehouse.alias("warehouse").merge(
            job_df.alias("job"),
            condition=merge_conditions
        ).whenNotMatchedInsertAll() \
        .whenMatchedUpdate(set=update_cols).execute()
    else:
        job_df.write.format("delta").save(warehouse_table)
