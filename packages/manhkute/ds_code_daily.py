import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.executors import LocalExecutor
from airflow.operators.subdag_operator import SubDagOperator
from idea.ds_task import DSTaskBuilder

from idea import sub_dag, const

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
    'start_date': datetime(2018, 12, 8),
    'end_date': datetime(2019, 12, 31),
}

dag = DAG('ds-code-daily',
          default_args=default_args,
          max_active_runs=3,
          schedule_interval=timedelta(1),
          start_date=datetime(2018, 12, 8),
          end_date=datetime(2019, 12, 31))

with open("/dfs/data/airflow/dags/config/cleaned-data-sensors.json") as f:
    data_clean_sensors = f.read()
json_cleaned_sensors = json.loads(data_clean_sensors)
check_cleaned_datasets_tasks = SubDagOperator(
    subdag=sub_dag.check_cleaned_datasets_dag(parent_dag_name='ds-code-daily',
                                              child_dag_name='cleaned-datasets',
                                              start_date=dag.start_date,
                                              schedule_interval=dag.schedule_interval,
                                              json_cleaned_sensors=json_cleaned_sensors),
    task_id='cleaned-datasets',
    dag=dag,
    executor=LocalExecutor())

with open("/dfs/data/airflow/dags/config/daily.json") as file:
    data_daily_tasks = file.read()
json_vertices = json.loads(data_daily_tasks)
ds_task = DSTaskBuilder(dag=dag)
ds_daily_tasks = ds_task.create_workflow_from_config(config=json_vertices, name="daily")

check_cleaned_datasets_tasks >> ds_daily_tasks[0]
