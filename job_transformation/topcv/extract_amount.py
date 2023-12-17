from pyspark.sql import types as T
from pyspark.sql import functions as F


@F.udf(returnType=T.FloatType())
def get_yoe(amount: str):
    return float(amount.split(' ')[0])
    