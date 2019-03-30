from datetime import date

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from contrib.dataset import HdfsDataset
import idea.command as cmd
from contrib.hdfs_sensor import HdfsSensorWithFlag, HdfsSensorGen
from idea.const import check_bi_pool, cleanup_pool, spark_pool
from idea.task_builder import TaskBuilder
from state_machine.vertex import Vertex


class ETLOperator(object):
    def __init__(self, dag, task_id, params):
        self.dag = dag
        self.params = params
        self.task_id = task_id

    def create_operator(self):
        pass


class HdfsSensorOperator(ETLOperator):
    def __init__(self, dag, task_id, params):
        super(HdfsSensorOperator, self).__init__(dag, task_id, params)

    def create_operator(self):
        etl_task_builder = ETLTaskBuilder(dag=self.dag)
        return etl_task_builder.create_hdfs_sensor_gen(
            flag=self.params["flag"],
            recursive=self.params["recursive"],
            min_size=self.params["min_size"],
            depends_on_past=self.params["depends_on_past"],
            file_path=self.params["file_path"],
            task_id=self.task_id,
            pool=self.params["pool"]
        )


class ETLBashOperator(ETLOperator):
    def __init__(self, dag, task_id, params):
        super(ETLBashOperator, self).__init__(dag, task_id, params)

    @TaskBuilder.ignore_failed
    def create_operator_failed(self, task_id):
        command = " ".join(self.params["command"])
        result = BashOperator(
            bash_command=command,
            dag=self.dag,
            pool=self.params["pool"],
            task_id=task_id)
        return result, result

    def create_operator(self):
        return self.create_operator_failed(task_id=self.task_id, is_ignore_failed=self.params["can_false"])


class ETLTaskBuilder(TaskBuilder):
    def __init__(self, bi_file_pattern="", opt_out_file_pattern="",
                 all_mapping_file_pattern="",
                 all_msisdn_datasets=[],
                 big_datasets=[],
                 all_non_msisdn_datasets=[],
                 *args, **kwargs):
        super(ETLTaskBuilder, self).__init__(*args, **kwargs)
        self.all_mapping_file_pattern = all_mapping_file_pattern
        self.all_msisdn_datasets = all_msisdn_datasets
        self.big_datasets = big_datasets
        self.bi_file_pattern = bi_file_pattern
        self.opt_out_file_pattern = opt_out_file_pattern
        self.all_non_msisdn_datasets = all_non_msisdn_datasets

    def get_opt_out_dataset(self, **kwargs):
        d = kwargs.get('ds')
        return HdfsDataset(path=self.opt_out_file_pattern,
                           frequency=7, start_date=date(2018, 7, 6), end_date=d, flag="_SUCCESS").get_latest().replace("=", "\\\\=")

    def get_opt_out_task(self):
        result = PythonOperator(
            task_id="get-opt-out-dataset",
            python_callable=self.get_opt_out_dataset,
            provide_context=True,
            dag=self.dag
        )
        return result, result

    def get_latest_mapping_task(self):
        result = PythonOperator(
            task_id="get-latest-mapping",
            python_callable=self.get_latest_mapping_dataset,
            provide_context=True,
            dag=self.dag
        )
        return result, result

    def get_latest_mapping_dataset(self, **kwargs):
        d = kwargs.get("ds")
        return HdfsDataset(path=self.all_mapping_file_pattern,
                           frequency=1,
                           start_date=date(2018, 10, 1), end_date=d, flag="_SUCCESS").get_latest()

    def generate_new_mapping(self):
        result = BashOperator(bash_command=cmd.generate_new_mapping_cmd,
                              dag=self.dag,
                              pool=cleanup_pool,
                              task_id="generate-new-mapping")
        return result, result

    def create_hdfs_sensor(self, flag, recursive, file_path, task_id):
        result = HdfsSensorWithFlag(flag=flag,
                                    recursive=recursive,
                                    filepath=file_path,
                                    task_id="check-{}".format(task_id),
                                    dag=self.dag)
        return result, result

    def create_hdfs_sensor_gen(self, flag, recursive, file_path, min_size, task_id, depends_on_past=False, pool=check_bi_pool):
        result = HdfsSensorGen(flag=flag,
                               recursive=recursive,
                               filepath=file_path,
                               min_size=min_size,
                               task_id=task_id,
                               dag=self.dag,
                               pool=pool,
                               depends_on_past=depends_on_past)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_count_task(self, command, task_id, priority_weight=1, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=command,
            dag=self.dag,
            task_id="count-{}".format(task_id),
            priority_weight=priority_weight,
            pool=cleanup_pool
        )
        return result, result

    @DeprecationWarning
    def create_check_yesterday_mapping(self):
        # TODO remove
        result = HdfsSensorWithFlag(flag="_SUCCESS",
                                    recursive=False,
                                    filepath=self.all_mapping_file_pattern.format(
                                        date="{{ execution_date.strftime('%Y%m%d') }}"),
                                    dag=self.dag,
                                    pool=check_bi_pool,
                                    task_id="check-yesterday-mapping")
        return result, result

    @DeprecationWarning
    def create_merge_mapping(self):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.merge_mapping_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                      yesterday="""{{ execution_date.strftime('%Y%m%d') }}"""),
            dag=self.dag,
            pool=cleanup_pool,
            task_id="merge-mapping")
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_mapping_for_rocksdb(self, task_id, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.mapping_for_rocksdb_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                            partitions=1,
                                                            col1="CID",
                                                            col2="MSISDN"),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_count_new_mapping(self, task_id, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.count_new_mapping_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}"""),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_count_all_mapping(self, task_id, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.count_all_mapping_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}"""),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id)
        return result, result

    def create_exclude_opt_out(self, ds):
        result = BashOperator(
            bash_command=cmd.exclude_opt_out_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                        dataset=ds,
                                                        partitions=20,
                                                        opt_out_path="""{{ task_instance.xcom_pull(task_ids='get-opt-out-dataset') }}"""),
            dag=self.dag,
            pool=cleanup_pool,
            task_id="exclude-opt-out-{}".format(ds))
        return result, result

    def create_replace_msisdn(self, ds):
        result = BashOperator(
            bash_command=cmd.replace_msisdn_cid_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                           dataset=ds),
            dag=self.dag,
            pool=cleanup_pool,
            task_id="replace-msisdn-cid-{}".format(ds))
        return result, result

    def create_cleanup(self, ds):
        if ds in self.all_non_msisdn_datasets:
            command = cmd.cleanup_non_msisdn_cmd
        elif ds in self.big_datasets:
            command = cmd.cleanup_big_datasets_cmd
        else:
            command = cmd.cleanup_cmd
        result = BashOperator(
            bash_command=command.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                        dataset=ds),
            dag=self.dag,
            pool=cleanup_pool,
            task_id="cleanup-{}".format(ds))
        return result, result

    @TaskBuilder.ignore_failed
    def create_fs_cleanup(self, task_id, ds, **kwargs):
        result = BashOperator(bash_command=cmd.fs_cleanup_cmd.format(dataset=ds,
                                                                     date="""{{ next_execution_date.strftime('%Y%m%d') }}"""),
                              dag=self.dag,
                              pool=cleanup_pool,
                              task_id=task_id)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_count_cleanup(self, task_id, ds, **kwargs):
        # TODO remove
        result = BashOperator(bash_command=cmd.count_cleanup_cmd.format(dataset=ds,
                                                                        date="""{{ next_execution_date.strftime('%Y%m%d') }}"""),
                              dag=self.dag,
                              pool=cleanup_pool,
                              task_id=task_id)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_count_unique_user(self, task_id, ds, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.count_unique_user_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                          dataset=ds),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id)
        return result, result

    @DeprecationWarning
    @TaskBuilder.ignore_failed
    def create_archive_cleanup(self, task_id, ds, **kwargs):
        # TODO remove
        result = BashOperator(
            bash_command=cmd.archive_cleanup_cmd.format(date="""{{ next_execution_date.strftime('%Y%m%d') }}""",
                                                        dataset=ds),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    def create_accumulate_subs_info(self, date, ds, task_id, yesterday, **kwargs):
        result = BashOperator(
            bash_command=cmd.accumulate_subs_info_cmd.format(date=date, dataset=ds, yesterday=yesterday,
                                                             partitionBy="sid_group"),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    @TaskBuilder.ignore_failed
    def create_count_accumulate_subs_info(self, date, ds, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.count_accumulate_subs_info.format(date=date, dataset=ds),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    def create_accumulate_opt_dataset(self, date, dataset, task_id, yesterday, **kwargs):
        result = BashOperator(
            bash_command=cmd.accumulate_opt_data_cmd.format(date=date, dataset=dataset, yesterday=yesterday),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    def create_replace_msisdn_opt_dataset(self, date, dataset, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.replace_msisdn_cid_opt_dataset_cmd.format(date=date, dataset=dataset),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    def create_cleanup_opt_dataset(self, date, dataset, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.cleanup_opt_dataset_cmd.format(date=date, dataset=dataset),
            dag=self.dag,
            pool=cleanup_pool,
            task_id=task_id
        )
        return result, result

    def create_prepare_dnd(self, date, task_id, **kwargs):
        result = BashOperator(
            bash_command="""ssh -t admin@172.30.250.1 "sh /home/admin/oozie-jobs/prepare-dnd/move.sh %s" """ % date,
            task_id=task_id,
            dag=self.dag,
            pool=check_bi_pool
        )
        return result, result

    def create_replace_msisdn_dnd(self, date, dataset, mapping_all, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.replace_msisdn_dnd_cmd.format(date=date, dataset=dataset, mapping_all=mapping_all),
            task_id=task_id,
            dag=self.dag,
            pool=cleanup_pool
        )
        return result, result

    def create_cleanup_dnd(self, date, dataset, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.cleanup_dnd_cmd.format(date=date, dataset=dataset),
            task_id=task_id,
            dag=self.dag,
            pool=cleanup_pool
        )
        return result, result

    @TaskBuilder.ignore_failed
    def create_count_score_subscribers(self, date, dataset, task_id, **kwargs):
        result = BashOperator(
            bash_command=cmd.count_score_subscribers_cmd.format(date=date, dataset=dataset),
            task_id=task_id,
            dag=self.dag,
            pool=spark_pool
        )
        return result, result

    def create_workflow_from_config(self, config, name):
        # TODO merge with ds_task.create_workflow_from_config
        vertices = {}
        operators = {}

        def create_operator_upstreams(vertex):
            print(vertices[vertex].upstreams)
            print(operators[vertex])
            if len(vertices[vertex].upstreams) > 0:
                for idx in vertices[vertex].upstreams:
                    operators[vertex][0].set_upstream(operators[idx][1])
            else:
                operators[vertex][0].set_upstream(start_dummy)

            down_stream = False
            for ii in vertices:
                if vertices[vertex].name in vertices[ii].upstreams:
                    down_stream = True
            if not down_stream:
                end_dummy.set_upstream(operators[vertex][1])

        def create_vertex(d_vertex):
            vertex = Vertex(name=d_vertex['name'], upstreams=d_vertex['upstreams'], params=d_vertex["params"])
            try:
                vertices[vertex.name]
            except KeyError:
                vertices[vertex.name] = vertex

        def operator_factory(task_id, params):
            if params["op_type"] == "hdfs-sensor":
                return HdfsSensorOperator(dag=self.dag, task_id=task_id, params=params)
            elif params["op_type"] == "bash":
                return ETLBashOperator(dag=self.dag, task_id=task_id, params=params)

        def create_operator(vertex):
            operators[vertices[vertex].name] = operator_factory(task_id=vertices[vertex].name, params=vertices[vertex].params).create_operator()

        start_dummy = DummyOperator(dag=self.dag, task_id="start-{name}".format(name=name))
        end_dummy = DummyOperator(dag=self.dag, task_id="end-{name}".format(name=name))
        list(map(lambda x: create_vertex(x), config['tasks']))
        list(map(lambda x: create_operator(x), vertices))
        list(map(lambda x: create_operator_upstreams(x), operators))

        return start_dummy, end_dummy
