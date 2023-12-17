from enum import Enum

class DBConfig:
    host = "localhost"
    port = 5432
    user = "postgres"
    password = "postgres"
    database = "recruit"
    table = "job_maket"
    connection_url = f"{host}:{port}"
