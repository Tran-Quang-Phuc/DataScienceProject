import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def extract_job_address(input: str):
    if '&' in input:
        return input.split('&')[0]
    return input.split(",")[0]
