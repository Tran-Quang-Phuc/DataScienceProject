from pyspark.sql import types as T
from pyspark.sql import functions as F


@F.udf(returnType=T.FloatType())
def get_yoe(job_experience_required: str):
    unit = job_experience_required.split(" ")
    if unit[0].lower() == "trên":
        return float(unit[1])

    return float(unit[0])
    
    

