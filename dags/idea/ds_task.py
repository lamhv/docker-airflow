from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from idea import const

from contrib.hdfs_sensor import HdfsSensorWithCount, HdfsSensorWithFlag
from idea.const import *
from idea.output_config import output
from idea.task_builder import TaskBuilder
from state_machine.vertex import Vertex


class DSTaskBuilder(TaskBuilder):
    """
    Builder data science task
    """

    def create_ds_task(self, name, start=None, priority_weight=1, mod=None):
        """
        Create ds task
        :param priority_weight:
        :param name: name of task
        :param start: number of task
        :return: BashOperator
        """
        if start is None:
            start = 'all'
        else:
            start = str(start)
        date = "{{ next_execution_date.strftime('%Y%m%d') }}"
        bash_command = """ssh -t admin@172.30.250.172 "sh %s/scripts/run.sh %s/scripts/notebooks/%s.py %s %s %s" """ % (
            const.local_path, const.local_path, name, date, start, mod)
        return BashOperator(task_id='run-%s-%s' % (name, start),
                            bash_command=bash_command,
                            dag=self.dag,
                            pool=spark_pool,
                            priority_weight=priority_weight)

    def check_output_task(self, name):
        """
        Create check output task
        :param name: name of task
        :return: (BashOperator, BashOperator)
        """
        date = "{{ next_execution_date.strftime('%Y%m%d') }}"
        try:
            if len(output[name]) == 1:
                path = '%s%s' % (const.data_working_path, output[name][0][0])
                nod = output[name][0][1]
                nof = output[name][0][2]
                bash_command = """ssh -t admin@172.30.250.172 "sh %s/scripts/check-output.sh %s%s %s %s" """ % (
                    const.local_path, path, date, nod, nof)
                task = BashOperator(task_id='check-output-%s' % name, bash_command=bash_command, dag=self.dag,
                                    pool=check_output_pool, priority_weight=2)
                return task, task
            else:
                t_dummy = DummyOperator(task_id='start-check-output-%s' % name, dag=self.dag)
                t_e_dummy = DummyOperator(task_id='end-check-output-%s' % name, dag=self.dag)
                for i in range(len(output[name])):
                    op = output[name][i]
                    path = '%s%s' % (const.data_working_path, op[0])
                    nod = op[1]
                    nof = op[2]
                    bash_command = """ssh -t admin@172.30.250.172 "sh %s/scripts/check-output.sh %s%s %s %s" """ % (
                        const.local_path, path, date, nod, nof)
                    t_dummy >> BashOperator(task_id='check-output-%s-%s' % (name, i), bash_command=bash_command,
                                            dag=self.dag, pool=check_output_pool) >> t_e_dummy
                return t_dummy, t_e_dummy
        except KeyError:
            dummy = DummyOperator(dag=self.dag, task_id="dummy-check-output-%s" % name)
            return dummy, dummy

    def create_task_with_check_output(self, name, num_of_task=None, priority_weight=1):
        """
        Create task with check output task
        :param parallel_mod:
        :param name:
        :param num_of_task:
        :param priority_weight:
        :return:
        """
        t_check_output_task = self.check_output_task(name)
        if num_of_task is None:
            t_ds_task = self.create_ds_task(name, priority_weight=priority_weight)
            t_ds_task >> t_check_output_task[0]
            return t_ds_task, t_check_output_task[1]
        else:
            t_dummy = DummyOperator(dag=self.dag, task_id='start-task-%s' % name)
            for t in range(num_of_task):
                t_dummy >> self.create_ds_task(name=name, start=t, mod=num_of_task) >> t_check_output_task[0]
            return t_dummy, t_check_output_task[1]

    def create_bash_operator(self, command, task_id):
        task = BashOperator(bash_command=command,
                            dag=self.dag,
                            task_id=task_id, pool=spark_pool)
        return task, task

    def create_hdfs_sensors_with_days(self, name, path, frequency=1, days=0):
        """
        Create sensors to check path that has format /path/to/date=<day> with <day> between now and previous days
        Number of sensors that are created equals days parameter
        Uses HdfsSensorWithFlag to check path
        :param name: name of task
        :param path: file pattern. Ex: /path/to/file/upto_date={}
        :param frequency: frequency of data
        :param days: number of day previous now that need to check
        :return: DummyOperator, DummyOperator
        """
        if days == 0:
            date = """{{ next_execution_date.strftime("%Y%m%d") }}"""
            p = const.data_working_path + path.format(date)
            sensor = HdfsSensorWithFlag(
                filepath=p,
                hdfs_conn_id='hdfs_default',
                dag=self.dag,
                pool=check_output_pool,
                task_id='check-input-%s' % name,
                flag="SUCCESS"
            )
            return sensor, sensor
        else:
            start_dummy = DummyOperator(dag=self.dag, task_id='start-%s' % name)
            end_dummy = DummyOperator(dag=self.dag, task_id='end-%s' % name)
            for i in range(days + 1):
                date = """{{ (next_execution_date - macros.timedelta(days=""" + str(
                    i * frequency) + """)).strftime('%Y%m%d') }}"""
                p = path.format(date)
                start_dummy >> HdfsSensorWithFlag(
                    filepath=p,
                    hdfs_conn_id='hdfs_default',
                    pool=check_output_pool,
                    dag=self.dag,
                    task_id='check-input-%s-%s' % (name, str(i)),
                    flag="SUCCESS"
                ) >> end_dummy
            return start_dummy, end_dummy

    def create_hdfs_sensors_with_count(self, name, path, total_path, total_files, frequency=1, days=0):
        # TODO Need to merge with create_hdfs_sensors_with_days function
        """
        Similar create_hdfs_sensors_with_days function, but it uses HdfsSensorWithCount instead of HdfsSensorWithFlag
        :param name: name of task
        :param path: file pattern. Ex: /path/to/file/upto_date={}
        :param total_path: total paths when use hdfs dfs -count -q
        :param total_files: total files when use hdfs dfs -count -q
        :param frequency: frequency of data
        :param days: number of day previous now that need to check
        :return: DummyOperator, DummyOperator
        """
        if days == 0:
            date = """{{ next_execution_date.strftime("%Y%m%d") }}"""
            p = path.format(date)
            sensor = HdfsSensorWithCount(
                filepath=p,
                hdfs_conn_id='hdfs_default',
                directory_count=total_path,
                file_count=total_files,
                dag=self.dag,
                pool=check_output_pool,
                task_id='check-input-%s' % name
            )
            return sensor, sensor
        else:
            start_dummy = DummyOperator(dag=self.dag, task_id='start-%s' % name)
            end_dummy = DummyOperator(dag=self.dag, task_id='end-%s' % name)
            for i in range(days + 1):
                date = """{{ (next_execution_date - macros.timedelta(days=""" + str(
                    i * frequency) + """)).strftime('%Y%m%d') }}"""
                p = path.format(date)
                start_dummy >> HdfsSensorWithCount(
                    filepath=p,
                    hdfs_conn_id='hdfs_default',
                    directory_count=total_path,
                    file_count=total_files,
                    pool=check_output_pool,
                    dag=self.dag,
                    task_id='check-input-%s-%s' % (name, str(i))
                ) >> end_dummy
            return start_dummy, end_dummy

    def create_hdfs_sensor_with_flag(self, path, flag, task_id, date):
        """
        Create hdfs sensor with flag
        :param path: file pattern. Ex: /path/to/file/upto_date={}
        :param flag: flag, ex: _SUCCESS
        :param task_id: name of task
        :param date: date of data
        :return: HdfsSensorWithFlag, HdfsSensorWithFlag
        """
        sensor = HdfsSensorWithFlag(filepath=path.format(date),
                                    flag=flag,
                                    dag=self.dag,
                                    task_id=task_id)
        return sensor, sensor

    def create_workflow_from_config(self, config, name):
        vertices = {}
        operators = {}

        def create_operator_upstreams(vertex):
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
            vertex = Vertex(name=d_vertex['name'], upstreams=d_vertex['upstreams'], parallel=d_vertex['parallel'])
            try:
                vertices[vertex.name]
            except KeyError:
                vertices[vertex.name] = vertex

        def create_operator(vertex):
            if vertices[vertex].parallel is None:
                operators[vertices[vertex].name] = self.create_task_with_check_output(name=vertices[vertex].name)
            else:
                operators[vertices[vertex].name] = self.create_task_with_check_output(name=vertices[vertex].name, num_of_task=vertices[vertex].parallel["mod"])

        start_dummy = DummyOperator(dag=self.dag, task_id="start-{name}".format(name=name))
        end_dummy = DummyOperator(dag=self.dag, task_id="end-{name}".format(name=name))
        list(map(lambda x: create_vertex(x), config['tasks']))
        list(map(lambda x: create_operator(x), vertices))
        list(map(lambda x: create_operator_upstreams(x), operators))

        for i in vertices:
            print(vertices[i].name)
            print(vertices[i].upstreams)

        return start_dummy, end_dummy
