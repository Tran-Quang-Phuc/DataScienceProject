import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_employment_type(employment_type: str):
    if "bán thời gian" in employment_type:
        return "part time"
    elif "hợp đồng" in employment_type:
        return "contract"
    return "full time"


@F.udf(returnType=T.BooleanType())
def is_temporory_job(employment_type: str):
    if "tạm thời" in employment_type:
        return True
    return False
