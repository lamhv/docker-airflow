import json
from datetime import datetime, timedelta

from airflow import DAG

from idea.etl_task import ETLTaskBuilder

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 12, 23),
    'end_date': datetime(2019, 12, 31),
}

dag = DAG('de-etl-daily', default_args=default_args, schedule_interval=timedelta(1))

with open("/dfs/data/airflow/dags/config/cleanup-daily.json") as f:
    data_cleanup_jobs = f.read()
json_cleanup_jobs = json.loads(data_cleanup_jobs)

opt_out_file_pattern = "/idea/bi/data_opt_in_out_accumulated/opt_out_accumulated/trx_date={date}"
etl_task_builder = ETLTaskBuilder(opt_out_file_pattern=opt_out_file_pattern,
                                  dag=dag)
etl_daily_tasks = etl_task_builder.create_workflow_from_config(config=json_cleanup_jobs, name="daily")

get_opt_out = etl_task_builder.get_opt_out_task()
get_opt_out[1] >> etl_daily_tasks[0]
