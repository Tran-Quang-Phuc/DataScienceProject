import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_seniority_level(job_function: str):
    if "Thực tập sinh" in job_function:
        return "Thực tập sinh"
    elif "/" in job_function:
        return job_function.split("/")[0].strip()
    return "Nhân viên"
