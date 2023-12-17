from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from tasks.check_file_exists import find_matching_file


source="careerlink"
daily_table = f"/home/phuc/Practice/DataScience/DSProject/data/raw/{source}/{source}_2023-12-04T09-33-38+00-00.jsonl"
ingest_table = f"/home/phuc/Practice/DataScience/DSProject/data/ingestion/{source}"

with DAG(
    dag_id="career_link_pipepline_test",
    start_date=datetime(2022,7,28),
    schedule=timedelta(minutes=1200),
    catchup=False,
    tags= ["career_link", "job"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=3)
    }
) as dag:
    # t0 = BashOperator(
    #     task_id = "crawl_data",
    #     bash_command = "scrapy crawl topcv",
    #     cwd = "home/phuc/Practice/DataScience/DSProject/job_cralwer"
    # )

    t1 = PythonOperator(
        task_id = "check_file_exists",
        python_callable = find_matching_file,
        op_args=[
            f"/home/phuc/Practice/DataScience/DSProject/data/raw/{source}",
            "*2023-12-16*"
        ]
    )

    t2 = BashOperator(
        task_id = "clean_data_and_ingest",
        bash_command=f"python3 {source}/transformation_and_ingestion.py {daily_table} {ingest_table}",
        cwd="/home/phuc/Practice/DataScience/DSProject/job_transformation"
    )

    t1 >> t2
