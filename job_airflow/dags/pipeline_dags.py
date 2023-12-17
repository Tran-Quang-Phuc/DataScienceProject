from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from tasks.check_file_exists import find_matching_file


def create_custom_dag(dag_name, schedule_interval, source):
    '''
    source is crawler name, must be the same with forder in job_transformation
    '''

    pattern = f"*{datetime.now().date()}*"
    raw_folder = f"/home/phuc/Practice/DataScience/DSProject/data/raw/{source}"
    ingest_table = f"/home/phuc/Practice/DataScience/DSProject/data/ingestion/{source}"
    warehouse_table = "/home/phuc/Practice/DataScience/DSProject/data/warehouse"

   
    # Create a new DAG dynamically
    with  DAG(
        dag_id="test_1",
        start_date=datetime(2022,7,28),
        schedule=timedelta(minutes=1200),
        catchup=False,
        tags= ["career_link", "job"],
        default_args={
            "retries": 3,
            "retry_delay": timedelta(minutes=3)
        }
    ) as dag:

        t0 = BashOperator(
            task_id = f"crawl_{source}",
            bash_command = f"scrapy crawl {source}",
            cwd = "/home/phuc/Practice/DataScience/DSProject/job_cralwer"
        )

        t1 = PythonOperator(
            task_id = "check_daily_file_exists",
            python_callable = find_matching_file,
            op_args = [raw_folder, pattern]
        )

        t2 = BashOperator(
            task_id = "clean_data_and_ingest",
            bash_command= f"python3 careerlink/transformation_and_ingestion.py $daily_table {ingest_table}",
            env={'daily_table': "{{ ti.xcom_pull(task_ids='check_daily_file_exists', key='daily_table') }}"},
            append_env=True,
            cwd="/home/phuc/Practice/DataScience/DSProject/job_transformation"
        )

        t3 = BashOperator(
            task_id = "upsert_to_warehouse",
            bash_command = f"python3 upsert_to_warehouse.py upsert-{source}-into-warehouse {ingest_table} {warehouse_table}",
            cwd = "/home/phuc/Practice/DataScience/DSProject/job_upsert_to_warehouse"
        )

        t0 >> t1 >> t2 >> t3

    return dag


sources = ["careerlink"]
for source in sources:
    create_custom_dag(dag_name=f"{source}_pipeline_1", schedule_interval=None, source=source)
