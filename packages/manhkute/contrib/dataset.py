import snakebite
from airflow.hooks.hdfs_hook import HDFSHook
import datetime
from datetime import timedelta


class HdfsDataset:

    def __init__(self, path, frequency, start_date, end_date, flag="", conn_id="hdfs_default", hook=HDFSHook):
        """

        :param path:
        :param frequency:
        :param start_date:
        :param flag:
        :param conn_id:
        :param hook:
        """
        self.conn_id = conn_id
        self.hook = hook
        self.path = path
        self.frequency = frequency
        self.start_date = start_date
        self.flag = flag
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    def get_latest(self, n=0):
        """

        :param n: n >= 0
        :return:
        """
        result = []
        sb = self.hook(self.conn_id).get_conn()
        today = self.end_date
        tmp = self.start_date
        d = timedelta(days=self.frequency)
        while tmp <= today:
            lop = [(self.path.format(date=tmp.strftime('%Y%m%d')))]
            try:
                list_of_path_in_hdfs = sb.ls(paths=lop, include_children=False, include_toplevel=True)
                result = result + [x['path'] for x in list_of_path_in_hdfs]
            except snakebite.errors.FileNotFoundException:
                pass
            except Exception as e:
                raise e
            tmp = tmp + d
        result.sort(reverse=True)
        print(result)
        return result[n]
