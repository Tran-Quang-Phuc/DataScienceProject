import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_employment_type(employment_type: str):
    if "Toàn thời gian" in employment_type:
        return "full time"
    return "part time"

