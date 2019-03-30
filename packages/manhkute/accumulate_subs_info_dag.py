from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from contrib.hdfs_sensor import HdfsSensorGen
from idea.const import check_bi_pool
from idea.etl_task import ETLTaskBuilder

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 10, 14),
    'end_date': datetime(2019, 12, 31)
}

dag = DAG('accumulate-subs-info-tasks',
          default_args=default_args,
          schedule_interval=timedelta(1),
          max_active_runs=1)

datasets = ['subs_info_post', 'subs_info_pre']
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

for ds in datasets:
    def create_accumulate_task(i):
        i = 6 - i
        accumulate_task = etl_task_builder.create_accumulate_subs_info(
            date="{{ (next_execution_date - macros.timedelta(days=" + str(i) + ")).strftime('%Y%m%d') }}",
            # date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
            ds=ds,
            yesterday="{{ (next_execution_date - macros.timedelta(days=" + str(i + 1) + ")).strftime('%Y%m%d') }}",
            task_id="accumulate-{dataset}-{i}".format(dataset=ds, i=i)
        )
        count_task = etl_task_builder.create_count_accumulate_subs_info(
            date="{{ (next_execution_date - macros.timedelta(days=" + str(i) + ")).strftime('%Y%m%d') }}",
            ds=ds,
            task_id="count-accumulate-{dataset}-{i}".format(dataset=ds, i=i)
        )
        accumulate_task[0] >> count_task[0]
        return accumulate_task[0], accumulate_task[0]

    sensor = HdfsSensorGen(
        dag=dag,
        flag='_SUCCESS',
        recursive=False,
        min_size=0,
        pool=check_bi_pool,
        task_id="check-{dataset}".format(dataset=ds),
        filepath='/data/clean/{dataset}/trx_date={date}'.format(dataset=ds, date="""{{ next_execution_date.strftime('%Y%m%d') }}""")
    )
    accumulate_tasks = etl_task_builder.create_sequential(list(map(create_accumulate_task, range(7))))
    start >> sensor >> accumulate_tasks[0]
