import typer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import *


app = typer.Typer()


@app.command()
def printf():
    print("Hello")


@app.command()
def write_to_delta_table():
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    quote = spark.read.format("json").load("/home/phuc/Practice/DataScience/DSProject/data/raw/quotes/quotes_2023-12-02T09-31-12+00-00.json")
    quote = quote.select(F.col("author"), F.col("tags"), F.col("text"))
    quote = quote.dropna()
    quote.write.format("delta").save("/home/phuc/Practice/DataScience/DSProject/data/delta/quotes")


if __name__ == "__main__":
    app()
