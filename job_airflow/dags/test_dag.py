from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2022,7,28),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags= ["tutorial"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:
    t0 = BashOperator(
        task_id = "print_folder",
        bash_command= "pwd"
    )

    t1 = BashOperator(
        task_id = "crawl_data",
        bash_command = "cd /home/phuc/Practice/DataScience/DSProject/job_airflow/dags && scrapy crawl quotes"
    )

    t2 = BashOperator(
        task_id = "save_data",
        bash_command = "cd /home/phuc/Practice/DataScience/DSProject/job_transformation && python3 test_spark.py write-to-delta-table"
    )

    t0 >> t1 >> t2
