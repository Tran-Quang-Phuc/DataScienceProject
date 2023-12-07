from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def merge_schema(
    spark: SparkSession,
    table: DeltaTable,
    df: DataFrame
):
    table_location = table.detail().collect()[0].location
    table = table.toDF()
    df_missing_columns = set(table.columns).difference(df.columns)
    df_missing_columns = df.withColumns({x: F.lit(None) for x in df_missing_columns})
    table_missing_columns = set(df.columns).difference(table.columns)
    if table_missing_columns:
        col_defs = []
        for column in table_missing_columns:
            col_defs.append(f"{column} {df.schema[column].dataType.simpleString()}")
        col_defs = ", ".join(col_defs)
        spark.sql(f"ALTER TABLE delta.`{table_location} ADD COLUMNS {col_defs}")
    table = DeltaTable.forPath(spark, table_location)
    return table, df
