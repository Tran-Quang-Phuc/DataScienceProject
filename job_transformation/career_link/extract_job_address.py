import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def extract_job_address(input: str):
    return input.split(",")[0]
