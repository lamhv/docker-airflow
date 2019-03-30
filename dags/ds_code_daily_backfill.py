import json
from datetime import timedelta, datetime

from airflow import DAG
from idea.ds_task import DSTaskBuilder

from idea import const

const.local_path = '/home/production/working/core_dev/cs_dev/lib_nb_converted/converted'
const.data_working_path = '/data/working/prod_v00'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2017, 4, 30),
    'end_date': datetime(2018, 10, 20),
}

dag = DAG('ds-code-daily-backfill-2018',
          default_args=default_args,
          max_active_runs=3,
          schedule_interval=timedelta(1),
          start_date=datetime(2017, 4, 30),
          end_date=datetime(2018, 10, 20))

with open("/dfs/data/airflow/dags/config/daily-backfill.json") as file:
    data_daily_tasks = file.read()
json_vertices = json.loads(data_daily_tasks)
ds_task = DSTaskBuilder(dag=dag)
ds_daily_tasks = ds_task.create_workflow_from_config(config=json_vertices, name="daily")
