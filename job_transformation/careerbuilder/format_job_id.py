import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def format_job_id(job_id: str):
    if job_id is None:
        return None
    elif len(job_id) > 9:
        return job_id[9:]
    return job_id
