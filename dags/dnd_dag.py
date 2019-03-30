from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from contrib.hdfs_sensor import HdfsSensorGen
from idea.etl_task import ETLTaskBuilder

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 19),
    'end_date': datetime(2019, 12, 31)
}

dag = DAG('dnd-tasks',
          default_args=default_args,
          schedule_interval="10 6 * * TUE,FRI",
          max_active_runs=3)

input_datasets = ['postpaid', 'prepaid']
bi_file_pattern = "/idea/bi/{}/trx_date={}"
opt_out_file_pattern = "/idea/bi/data_opt_in_out_accumulated/opt_out_accumulated/trx_date="

all_mapping_file_pattern = "/data/mapping/{date}/all"
all_msisdn_datasets = ["voice", "sms", "recharge", "payment", "dbal", "vas", "subs_info_pre", "subs_info_post",
                       "data_pre", "data_post"]
all_non_msisdn_datasets = ["cell_info", "tac"]
big_datasets = ["voice", "sms", "data_pre"]
etl_task_builder = ETLTaskBuilder(bi_file_pattern=bi_file_pattern,
                                  opt_out_file_pattern=opt_out_file_pattern,
                                  dag=dag,
                                  all_mapping_file_pattern=all_mapping_file_pattern,
                                  all_msisdn_datasets=all_msisdn_datasets,
                                  big_datasets=big_datasets,
                                  all_non_msisdn_datasets=all_non_msisdn_datasets)
start = DummyOperator(dag=dag, task_id='start')
move_task = etl_task_builder.create_prepare_dnd(
    date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
    task_id="move-increment-data"
)

sensor = HdfsSensorGen(
    dag=dag,
    flag='_SUCCESS',
    recursive=False,
    min_size=0,
    task_id="check-bi-dnd",
    filepath='/idea/bi/dnd/trx_date={date}'.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""")
)

get_mapping_dataset = etl_task_builder.get_latest_mapping_task()
replace_msisdn = etl_task_builder.create_replace_msisdn_dnd(
    date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
    dataset="dnd",
    mapping_all="""{{ task_instance.xcom_pull(task_ids='get-latest-mapping') }}""",
    task_id="replace-msisdn-dnd"
)
cleanup = etl_task_builder.create_cleanup_dnd(
    date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
    dataset="dnd",
    task_id="cleanup-dnd"
)

fs_clean = etl_task_builder.create_fs_cleanup(task_id="fs-cleanup-dnd", ds="dnd")

start >> move_task[0]
move_task[1] >> sensor
sensor >> get_mapping_dataset[0]
get_mapping_dataset[1] >> replace_msisdn[0]
replace_msisdn[1] >> cleanup[0]
cleanup[1] >> fs_clean[0]
