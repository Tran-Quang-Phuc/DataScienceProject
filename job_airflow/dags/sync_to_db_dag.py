from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from synchronize_to_database.upsert_to_postgres import upsert_spark_df_to_postgres


warehouse = "/home/phuc/Practice/DataScience/DSProject/data/warehouse"
table_name = "job_market"
table_unique_keys = ["job_id"],
database_credentials = {
    "host": "localhost",
    "database": "datascience",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}
batch_size = 100
parallelism = 1


with DAG(
    dag_id=f"sync_to_db",
    start_date=datetime(2022,7,28),
    schedule=timedelta(minutes=1200),
    catchup=False,
    tags= ["daily", "job"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=3)
    }
) as dag:
    t0 = PythonOperator(
        task_id="sync_to_postgres",
        python_callable=upsert_spark_df_to_postgres,
        op_args=[
            warehouse,
            table_name,
            table_unique_keys,
            database_credentials,
            batch_size,
            parallelism
        ]
    )