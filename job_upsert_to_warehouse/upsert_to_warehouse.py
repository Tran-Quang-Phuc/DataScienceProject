import typer
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from job_upsert_to_warehouse.common.upsert_to_warehouse import ColumnInfo
from utils.config_spark_delta import config_spark_delta
from job_upsert_to_warehouse.common.upsert_to_warehouse import upsert_to_warehouse


app = typer.Typer()


@app.command()
def upsert_careerlink_into_warehouse(
    ingest_table: str,
    warehouse_table: str,
    ingest_ids=None
):
    builder = SparkSession.builder.master("local")
    spark = config_spark_delta(builder)

    career_link_df = spark.read.format("delta").load(ingest_table)
    if ingest_ids:
        career_link_df = career_link_df.filter(F.col("ingest_id").isin(ingest_ids))

    upsert_to_warehouse(
        spark,
        career_link_df,
        warehouse_table,
        columns=[
            ColumnInfo("post_id", "id"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("industry", "industry"),
            ColumnInfo("company_name", None),
            ColumnInfo("education_level", None),
            ColumnInfo("employment_type", None),
            ColumnInfo("gender", "gender"),
            ColumnInfo("job_address", "address"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_deadline", "deadline"),
            ColumnInfo("job_experience_requied", "experience_required"),
            ColumnInfo("job_yoe_min", "yoe_min"),
            ColumnInfo("job_yoe_max", "yoe_max"),
            ColumnInfo("job_listed", "post_date"),
            ColumnInfo("salary", None),
            ColumnInfo("salary_min", None),
            ColumnInfo("salary_max", None),
            ColumnInfo("skill", None),
            ColumnInfo("job_level", "level"),
        ]
    )


@app.command()
def upsert_careerbuilder_into_warehouse(
    ingest_table: str,
    warehouse_table: str,
    ingest_ids=None
):
    builder = SparkSession.builder.master("local")
    spark = config_spark_delta(builder)

    careerbuilder_df = spark.read.format("delta").load(ingest_table)
    if ingest_ids:
        careerbuilder_df = careerbuilder_df.filter(F.col("ingest_id").isin(ingest_ids))

    upsert_to_warehouse(
        spark,
        careerbuilder_df,
        warehouse_table,
        columns=[
            ColumnInfo("job_id", "id"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("industry", "industry"),
            ColumnInfo("company_name", None),
            ColumnInfo("education_level", None),
            ColumnInfo("employment_type", None),
            ColumnInfo("gender", "gender"),
            ColumnInfo("job_address", "address"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_deadline", "deadline"),
            ColumnInfo("job_experience_requied", "experience_required"),
            ColumnInfo("job_yoe_min", "yoe_min"),
            ColumnInfo("job_yoe_max", "yoe_max"),
            ColumnInfo("job_listed", "post_date"),
            ColumnInfo("salary", None),
            ColumnInfo("salary_min", None),
            ColumnInfo("salary_max", None),
            ColumnInfo("skill", None),
            ColumnInfo("job_level", "level"),
        ]
    )


@app.command()
def print_hello():
    print("hello")


if __name__ == "__main__":
    app()
