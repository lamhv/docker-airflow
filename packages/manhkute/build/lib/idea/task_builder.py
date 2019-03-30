from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


class TaskBuilder(object):
    def __init__(self, dag, local_path="", data_working_path=""):
        self.dag = dag
        self.local_path = local_path
        self.data_working_path = data_working_path

    @staticmethod
    def create_sequential(list_of_task):
        """
        Create sequential tasks
        :param list_of_task: list of tasks
        :return: Tuple (first task, last task)
        """
        num_of_list = len(list_of_task)
        for j in range(num_of_list - 1):
            list_of_task[j][1] >> list_of_task[j + 1][0]
        return list_of_task[0][0], list_of_task[num_of_list - 1][1]

    def create_parallel(self, list_of_task, name):
        """
        Create tasks that run in parallel
        :param list_of_task: list of tasks
        :param name: group name of tasks
        :return: Tuple (Fork Dummy operator, Join Dummy operator)
        """
        start_dummy = DummyOperator(dag=self.dag, task_id='start-%s' % name)
        end_dummy = DummyOperator(dag=self.dag, task_id='end-%s' % name)
        for task in list_of_task:
            start_dummy >> task[0]
            task[1] >> end_dummy
        return start_dummy, end_dummy

    @staticmethod
    def ignore_failed(func):
        def wrap_func(obj, task_id, is_ignore_failed=True, **kwargs):
            if not is_ignore_failed:
                return func(obj, task_id=task_id, **kwargs)
            else:
                act_task = func(obj, task_id=task_id, **kwargs)
                dummy = DummyOperator(dag=obj.dag, task_id="dummy-" + task_id, priority_weight=3, trigger_rule=TriggerRule.DUMMY)
                act_task[1] >> dummy
                return act_task[0], dummy
        return wrap_func
