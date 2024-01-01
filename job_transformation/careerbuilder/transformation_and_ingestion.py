import typer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from delta import *
from job_transformation.careerbuilder.format_job_id import format_job_id
from job_transformation.careerbuilder.normalize_industries import normalize_industries
from job_transformation.careerlink.extract_job_address import extract_job_address
from utils.merge_schema import merge_schema
from job_transformation.careerbuilder.modify_company_name import modify_company_name
from job_transformation.careerbuilder.modify_job_title import modify_job_title
from job_transformation.careerbuilder.modify_job_listed import convert_to_job_listed_datetime
from job_transformation.careerbuilder.modify_job_deadline import convert_to_job_deadline_datetime
# from job_transformation.careerbuilder.modify_employment_type import normalize_employment_type
from job_transformation.careerbuilder.modify_salary import extract_min_max_salary
from job_transformation.careerbuilder.extract_min_max_yoe import extract_min_max_yoe
from job_transformation.careerbuilder.modify_seniority_level import normalize_seniority_level


app = typer.Typer()


@app.command()
def transform_and_ingest(
    daily_table: str,
    ingest_table: str
):
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    job_df = spark.read.csv(daily_table, header=True)
    job_df = job_df.filter(~F.col("job_id").contains("Năm")) # drop row that contains data is crawled not properly
    job_df = job_df.withColumn("job_id", format_job_id(job_df["job_id"]))
    job_df = job_df.withColumn("company_name", modify_company_name(job_df["company_name"]))
    job_df = job_df.withColumn("job_address", extract_job_address(job_df["company_location"]))
    job_df = job_df.withColumn("job_title", modify_job_title(job_df["job_title"]))
    job_df = job_df.withColumn("job_listed", convert_to_job_listed_datetime(job_df["job_listed"]))
    job_df = job_df.withColumn("job_deadline", convert_to_job_deadline_datetime(job_df["job_deadline"]))
    # job_df = job_df.withColumn("employment_type", normalize_employment_type(job_df["employment_type"]))
    job_df = job_df.withColumn("level", normalize_seniority_level(job_df["seniority_level"])).drop("seniority_level")
    job_df = job_df.withColumn('industry', normalize_industries(job_df["Industries"])).drop("Industries")
    job_df = extract_min_max_yoe(job_df, "experience", "job_yoe_min", "job_yoe_max")
    job_df = extract_min_max_salary(job_df, "salary", "salary_min", "salary_max")
    job_df = job_df.withColumn("ingested_at", F.current_date())
    job_df = job_df.withColumn("updated_at", F.current_date())

    if DeltaTable.isDeltaTable(spark, ingest_table):
        deltaTable = DeltaTable.forPath(spark, ingest_table)
        deltaTable, job_df = merge_schema(spark, deltaTable, job_df)
        delta_df = deltaTable.toDF()
        cols = {}
        for col in job_df.columns:
            if col == "ingested_at":
                continue
            cols[col] = F.when(job_df[col].isNotNull(), job_df[col]).otherwise(delta_df[col])
        deltaTable.alias("ingestion_table").merge(
            job_df.alias("daily_table"),
            "ingestion_table.job_id = daily_table.job_id"
        ).whenMatchedUpdate(set=cols).whenNotMatchedInsertAll().execute()
        
    else:
        job_df.write.format("delta").save(ingest_table)


if __name__ == "__main__":
    app()
