import typer
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from delta import *
from career_link.convert_job_listed_to_datetime import convert_to_job_listed_datetime
from career_link.extract_job_address import extract_job_address
from career_link.extract_job_deadline_date import extract_job_deadline_date
from career_link.normalize_employment_type import normalize_employment_type
from career_link.normalize_job_function import normalize_job_function
from career_link.extract_min_max_salary import extract_min_max_salary
from career_link.extract_min_max_yoe import extract_min_max_yoe
from common.merge_schema import merge_schema


app = typer.Typer()


@app.command()
def transform_and_ingest(
    daily_table: str,
    ingest_table: str,
):
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    job_df = spark.read.format("json").load(daily_table)
    job_df = job_df.withColumn("job_listed", convert_to_job_listed_datetime(job_df["job_listed"]))
    job_df = job_df.withColumn("job_address", extract_job_address(job_df["job_address"]))
    job_df = job_df.withColumn("job_deadline", extract_job_deadline_date(job_df["job_deadline"]))
    job_df = job_df.withColumn("employment_type", normalize_employment_type(job_df["employment_type"]))
    job_df = job_df.withColumn("job_role", normalize_job_function(job_df["job_function"])).drop("job_function")
    job_df = extract_min_max_yoe(job_df, "job_experience_requied", "job_yoe_min", "job_yoe_max")
    job_df = extract_min_max_salary(job_df, "salary", "salary_min", "salary_max")
    job_df["ingested_at"] = datetime.now()
    job_df["updated_at"] = datetime.now()

    if DeltaTable.isDeltaTable(spark, ingest_table):
        deltaTable = DeltaTable.forPath(spark, ingest_table)
        deltaTable, job_df = merge_schema(spark, deltaTable, job_df)
        cols = {}
        for col in job_df.columns:
            if col == "created_at":
                continue
            cols[col] = F.when(job_df[col].isNotNull(), job_df[col]).otherwise(deltaTable[col])
        deltaTable.alias("ingestion_table").merge(
            job_df.alias("daily_table"),
            "ingestion_table.job_id = daily_table.job_id"
        ).whenMatchedUpdate(set=cols).whenNotMatchedInsertAll()
        
    else:
        job_df.write.format("delta").mode("overwrite").save(ingest_table)


if __name__ == "__main__":
    app()
