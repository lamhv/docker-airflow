from datetime import datetime, timedelta

from airflow import DAG

from idea.etl_task import ETLTaskBuilder

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2018, 9, 6),
    'end_date': datetime(2019, 12, 31)
}

dag = DAG('cleanup-mexp',
          default_args=default_args,
          schedule_interval="0 10 6 * *",
          max_active_runs=1)

bi_file_pattern = "/idea/bi/{}/trx_date={}"
opt_out_file_pattern = "/idea/bi/data_opt_in_out_accumulated/opt_out_accumulated/trx_date={date}"
all_mapping_file_pattern = "/data/mapping/{}/all"
all_msisdn_datasets = ["mexp_post", "mexp_pre"]

etl_task_builder = ETLTaskBuilder(bi_file_pattern=bi_file_pattern,
                                  opt_out_file_pattern=opt_out_file_pattern,
                                  dag=dag,
                                  all_mapping_file_pattern=all_mapping_file_pattern,
                                  all_msisdn_datasets=all_msisdn_datasets
                                  )
get_opt_out = etl_task_builder.get_opt_out_task()


def create_task(x):
    sensor = etl_task_builder.create_hdfs_sensor_gen(
        flag="_SUCCESS",
        recursive=False,
        min_size=0,
        depends_on_past=True,
        file_path=bi_file_pattern.format(x, "{{ next_execution_date.strftime('%Y%m%d') }}"),
        task_id="check-bi-{}".format(x)
    )
    exclude_opt_out = etl_task_builder.create_exclude_opt_out(x)
    replace_msisdn_cid = etl_task_builder.create_replace_msisdn(x)
    cleanup = etl_task_builder.create_cleanup(x)
    fs_cleanup = etl_task_builder.create_fs_cleanup(ds=x, task_id="fs-cleanup-{}".format(x), is_ignore_failed=False)
    sensor[1] >> exclude_opt_out[0] >> replace_msisdn_cid[0] >> cleanup[0] >> fs_cleanup[0]
    return sensor[0], fs_cleanup[1]


tasks = list(map(create_task, all_msisdn_datasets))

check_bi_tasks = etl_task_builder.create_parallel(tasks, "check-bi")

get_opt_out[1] >> check_bi_tasks[0]
