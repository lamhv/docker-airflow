from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from contrib.hdfs_sensor import HdfsSensorGen
from idea.const import check_bi_pool
from idea.etl_task import ETLTaskBuilder

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 12),
    'end_date': datetime(2019, 12, 31)
}

dag = DAG('accumulate-opt-data-tasks',
          default_args=default_args,
          schedule_interval=timedelta(7),
          max_active_runs=1)

datasets = ['opt_in', 'opt_out']
input_datasets = ['postpaid', 'prepaid']
bi_file_pattern = "/idea/bi/{}/trx_date={}"
opt_out_file_pattern = "/idea/bi/data_opt_in_out_accumulated/opt_out_accumulated/trx_date="

all_mapping_file_pattern = "/data/mapping/{}/all"
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
end_sensor = DummyOperator(dag=dag, task_id='end-check-input')
mapping_sensor = HdfsSensorGen(
    dag=dag,
    flag="_SUCCESS",
    resources=False,
    min_size=0,
    task_id="check-mapping",
    filepath="/data/mapping/{date}/all".format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""")
)
start >> mapping_sensor >> end_sensor

for ds in input_datasets:
    sensor = HdfsSensorGen(
        dag=dag,
        flag='_SUCCESS',
        recursive=False,
        min_size=0,
        pool=check_bi_pool,
        task_id="check-{dataset}".format(dataset=ds),
        filepath='/data_opt_in_out/{dataset}/trx_date={date}'.format(dataset=ds, date="""{{ next_execution_date.strftime('%Y%m%d') }}""")
    )
    start >> sensor >> end_sensor

for ds in datasets:
    accumulate_task = etl_task_builder.create_accumulate_opt_dataset(
        date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
        dataset=ds,
        task_id="accumulate-{dataset}".format(dataset=ds),
        yesterday="""{{ execution_date.strftime('%Y%m%d') }}"""
    )

    replace_msisdn = etl_task_builder.create_replace_msisdn_opt_dataset(
        date="",
        dataset="{dataset}_accumulated".format(dataset=ds),
        task_id="replace-{dataset}_accumulated".format(dataset=ds)
    )

    cleanup = etl_task_builder.create_cleanup_opt_dataset(
        date="",
        dataset="{dataset}_accumulated".format(dataset=ds),
        task_id="cleanup-{dataset}_accumulated".format(dataset=ds)
    )
    fs_clean = etl_task_builder.create_fs_cleanup(
        ds="{dataset}_accumulated".format(dataset=ds),
        task_id="fs-clean-{dataset}_accumulated".format(dataset=ds)
    )

    end_sensor >> accumulate_task[0]
    accumulate_task[1] >> replace_msisdn[0]
    replace_msisdn[1] >> cleanup[0]
    cleanup[1] >> fs_clean[0]
