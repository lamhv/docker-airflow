import json
from datetime import timedelta
import datetime

from airflow import DAG
from airflow.executors import LocalExecutor
from airflow.operators.subdag_operator import SubDagOperator

from contrib.time_sensor import TimeSensor
from idea import sub_dag, const
from idea.ds_task import DSTaskBuilder

const.local_path = '/home/production/working/core_dev/cs_dev/lib_nb_converted/converted'
const.data_working_path = '/data/working/prod_v00'

print(const.location_path)
print(const.data_working_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('ds-code-weekly-backfill-2017-2018',
          default_args=default_args,
          schedule_interval=timedelta(7),
          max_active_runs=2,
          start_date=datetime.datetime(2017, 4, 30),
          end_date=datetime.datetime(2018, 11, 18))


with open("/dfs/data/airflow/dags/config/ds-daily-sensors-backfill.json") as f:
    data_daily_sensors = f.read()
json_daily_sensors = json.loads(data_daily_sensors)
check_ds_daily_datasets_tasks = SubDagOperator(
    subdag=sub_dag.check_ds_daily_datasets_dag(parent_dag_name='ds-code-weekly-backfill-2017-2018',
                                               child_dag_name='ds-daily-datasets',
                                               start_date=dag.start_date,
                                               schedule_interval=dag.schedule_interval,
                                               json_cleaned_sensors=json_daily_sensors),
    task_id='ds-daily-datasets',
    dag=dag,
    executor=LocalExecutor())


with open("/dfs/data/airflow/dags/config/weekly-backfill.json") as file:
    data_weekly_tasks = file.read()
json_vertices = json.loads(data_weekly_tasks)
ds_task = DSTaskBuilder(dag=dag)
ds_weekly_tasks = ds_task.create_workflow_from_config(json_vertices, "weekly")

check_ds_daily_datasets_tasks >> ds_weekly_tasks[0]