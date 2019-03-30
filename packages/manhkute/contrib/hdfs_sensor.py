from airflow.operators.sensors import HdfsSensor
import logging
import functools


class HdfsSensorWithFlag(HdfsSensor):
    def __init__(self, flag, recursive=False, *args, **kwargs):
        super(HdfsSensorWithFlag, self).__init__(timeout=60 * 60 * 24 * 14, *args, **kwargs)
        self.flag = flag
        self.recursive = recursive

    @staticmethod
    def filter_flag(result, flag):
        result = [x for x in result if flag in x['path']]
        return result

    def poke(self, context):
        sb = self.hook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        if not self.recursive:
            try:
                result = [f for f in sb.ls([self.filepath], include_toplevel=True)]
                result = self.filter_flag(result, self.flag)
            except Exception as e:
                logging.exception(e)
                return False
            return bool(result)
        else:
            status = False
            try:
                list_of_paths = [f['path'] for f in sb.ls([self.filepath], include_toplevel=False)]
                for i in list_of_paths:
                    status = True
                    result = [f for f in sb.ls([i], include_toplevel=False)]
                    result = self.filter_flag(result, self.flag)
                    if not bool(result):
                        status = status and False
                        break
            except Exception as e:
                logging.exception(e)
                return False
            return status


def filter_with_min_size(func):
    def wrap_func(obj, result, *args, **kwargs):
        result = func(obj, result, *args, **kwargs)
        logging.info(result)
        sb = obj.hook(obj.hdfs_conn_id).get_conn()
        try:
            sizes = [i['length'] for i in sb.du(result)]
            size = functools.reduce(lambda x, y: x + y, sizes)
            if size < obj.min_size:
                logging.info("size = %s < %s" % (size, obj.min_size))
                return []
            else:
                return result
        except Exception as ex:
            logging.exception(ex)
            return []

    return wrap_func


def filter_with_flag_recursive(func):
    def wrap_func(obj, result, *args, **kwargs):
        result = func(obj, result, *args, **kwargs)
        logging.info(result)
        if obj.recursive:
            status = False
            try:
                sb = obj.hook(obj.hdfs_conn_id).get_conn()
                list_of_paths = [f['path'] for f in sb.ls([obj.filepath], include_toplevel=False)]
                for i in list_of_paths:
                    status = True
                    result = [f['path'] for f in sb.ls([i], include_toplevel=False)]
                    result = filter_flag(result, obj.flag)
                    if not bool(result):
                        status = status and False
                        break
            except Exception as e:
                logging.exception(e)
                return []
            if status:
                return result
            return []
        return result

    return wrap_func


def filter_flag(result, flag):
    if [x for x in result if flag in x]:
        return result
    else:
        return []


def filter_with_flag(func):
    def wrap_func(obj, result, *args, **kwargs):
        logging.info(result)
        if not obj.recursive:
            result = func(obj, result, **kwargs)
            return filter_flag(result, obj.flag)
        else:
            return result
    return wrap_func


class HdfsSensorGen(HdfsSensor):
    def __init__(self, flag, recursive=False, min_size=0, *args, **kwargs):
        super(HdfsSensorGen, self).__init__(timeout=60 * 60 * 24 * 14, *args, **kwargs)
        self.flag = flag
        self.recursive = recursive
        self.min_size = min_size

    @filter_with_min_size
    @filter_with_flag_recursive
    @filter_with_flag
    def filter(self, result):
        return [x['path'] for x in result]

    def poke(self, context):
        sb = self.hook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        try:
            result = [f for f in sb.ls([self.filepath], include_toplevel=False)]
        except Exception as ex:
            logging.exception(ex)
            return False
        return bool(self.filter(result))


class HdfsSensorWithCount(HdfsSensor):
    def __init__(self, directory_count, file_count, *args, **kwargs):
        super(HdfsSensorWithCount, self).__init__(timeout=60 * 60 * 24 * 14, *args, **kwargs)
        self.directory_count = directory_count
        self.file_count = file_count

    @staticmethod
    def filter_count(result, directory_count, file_count):
        result = [x for x in result if x["directoryCount"] == directory_count and x["fileCount"] == file_count]
        return result

    def poke(self, context):
        sb = self.hook(self.hdfs_conn_id).get_conn()
        logging.getLogger("snakebite").setLevel(logging.WARNING)
        try:
            result = [f for f in sb.count([self.filepath])]
            result = self.filter_count(result, self.directory_count, self.file_count)
        except Exception as e:
            logging.exception(e)
            return False
        return bool(result)
