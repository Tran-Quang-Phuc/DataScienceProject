
import pyspark.sql.functions as F
import pyspark.sql.types as T
import re

@F.udf(returnType=T.StringType())
def modify_company_name(input: str):
    input = re.sub(r'\[.*?\]', '', input)
    input = re.sub(r'\(.*?\)', '', input)
    input = input.title()

    input = input.replace("Công Ty", "Cty")
    input = input.replace("Tnhh", "TNHH")
    input = input.replace("Cổ Phần", "CP")
    input = input.strip()

    return input


