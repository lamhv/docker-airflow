from datetime import date

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from contrib.dataset import HdfsDataset
from idea import const
from idea.command import score_encrypted, merged_location, create_subscriber, load_subscribers, load_credit_score, \
    alter_table_subscribers, alter_table_score, verify_score_task, create_tags

from idea.ds_task import DSTaskBuilder
from idea.etl_task import ETLTaskBuilder


def generate_for_leadgen_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag = create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval)
    generate_for_leadgen_builder = DSTaskBuilder(dag=sub_dag, local_path=const.local_path,
                                                 data_working_path="")
    etl_task_builder = ETLTaskBuilder(dag=sub_dag)
    DSTaskBuilder.create_sequential(
        [
            generate_for_leadgen_builder.create_parallel([
                generate_for_leadgen_builder.create_bash_operator(score_encrypted, "encrypt-score"),
                generate_for_leadgen_builder.create_bash_operator(merged_location, "merge-location")
            ], name="encrypt-score-and-location"),
            generate_for_leadgen_builder.create_bash_operator(create_tags, "create-tags"),
            generate_for_leadgen_builder.create_hdfs_sensors_with_count(name="create-tags"
                                                                        , path="/data/working/leadgen_team/sub_tags/combine_tag/upto_date={}"
                                                                        , total_path=21
                                                                        , total_files=60),
            generate_for_leadgen_builder.create_bash_operator(create_subscriber, "create-subscribers"),
            generate_for_leadgen_builder.create_parallel([
                etl_task_builder.create_count_score_subscribers(date="{{ next_execution_date.strftime('%Y%m%d') }}",
                                                                dataset="merged_location",
                                                                task_id="count-merged-location"),
                etl_task_builder.create_count_score_subscribers(date="{{ next_execution_date.strftime('%Y%m%d') }}",
                                                                dataset="score_encrypted",
                                                                task_id="count-score-encrypted"),
                etl_task_builder.create_count_score_subscribers(date="{{ next_execution_date.strftime('%Y%m%d') }}",
                                                                dataset="subscribers", task_id="count-subscribers")
            ], name="count-score-subscribers")
        ])
    return sub_dag


def load_into_mariadb(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag = create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval)
    load_into_mariadb_builder = DSTaskBuilder(dag=sub_dag, local_path=const.local_path,
                                              data_working_path=const.data_working_path)
    DSTaskBuilder.create_sequential(
        [load_into_mariadb_builder.create_bash_operator(load_subscribers, "load-subscribers"),
         load_into_mariadb_builder.create_bash_operator(load_credit_score, "load-creadit-score"),
         load_into_mariadb_builder.create_bash_operator(alter_table_subscribers, "alter-table-subscribers"),
         load_into_mariadb_builder.create_bash_operator(alter_table_score, "alter-table-score")])
    return sub_dag


def verify_score_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag = create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval)
    verify_score_builder = DSTaskBuilder(dag=sub_dag, local_path=const.local_path,
                                         data_working_path=const.data_working_path)

    def get_latest_accepted_score_task(**kwargs):
        accepted_score_file_pattern = "/data/score_lead_gen/score_accepted/upto_date={date}"
        d = kwargs.get('ds')
        accepted_path = HdfsDataset(path=accepted_score_file_pattern,
                           frequency=7, start_date=date(2018, 9, 2), end_date=d, flag="_SUCCESS").get_latest()
        accepted_date = accepted_path.split("=")[1]
        return "/data/working/prod_v00/creditscore/score_v01_01_active/upto_date\\\\={date}".format(date=accepted_date)

    def create_latest_accepted_score_task():
        get_latest_accepted_score = PythonOperator(python_callable=get_latest_accepted_score_task,
                                                   task_id="get-latest-score-accepted",
                                                   provide_context=True,
                                                   dag=sub_dag)
        return get_latest_accepted_score, get_latest_accepted_score

    def create_verify_score_task():
        verify_score = BashOperator(bash_command=verify_score_task,
                                    dag=sub_dag,
                                    task_id="verify-score", pool=const.spark_pool)
        return verify_score, verify_score

    DSTaskBuilder.create_sequential([create_latest_accepted_score_task(),
                                     create_verify_score_task(),
                                     verify_score_builder.create_hdfs_sensor_with_flag(
                                         path="/data/score_lead_gen/score_accepted/upto_date={}",
                                         date="{{ next_execution_date.strftime('%Y%m%d') }}",
                                         flag='_SUCCESS',
                                         task_id="check-score-accepted")])
    return sub_dag


def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    sub_dag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
        concurrency=10
    )
    return sub_dag


def check_ds_daily_datasets_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, json_cleaned_sensors):
    sub_dag = create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval)
    check_ds_daily_datasets_builder = DSTaskBuilder(dag=sub_dag, local_path=const.local_path,
                                                    data_working_path=const.data_working_path)
    for s in json_cleaned_sensors['sensors']:
        check_ds_daily_datasets_builder.create_hdfs_sensors_with_count(name=s['name'],
                                                                       path=s['path_pattern'],
                                                                       total_path=s['total_path'],
                                                                       total_files=s['total_files'],
                                                                       frequency=s['interval'],
                                                                       days=s['number_of_days'] - 1)
    return sub_dag


def check_cleaned_datasets_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, json_cleaned_sensors):
    sub_dag = create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval)
    check_cleaned_datasets_builder = DSTaskBuilder(dag=sub_dag, local_path=const.local_path,
                                                   data_working_path=const.data_working_path)
    start_dummy = DummyOperator(dag=sub_dag, task_id="start-dummy")
    tmp_operator = start_dummy
    for s in json_cleaned_sensors['sensors']:
        task = check_cleaned_datasets_builder.create_hdfs_sensors_with_days(name=s['name'],
                                                                     path=s['path_pattern'],
                                                                     frequency=s['interval'],
                                                                     days=s['number_of_days'] - 1)
        tmp_operator >> task[0]
        tmp_operator = task[1]
    return sub_dag
