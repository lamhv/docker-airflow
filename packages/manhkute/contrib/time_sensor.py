from airflow.operators.sensors import BaseSensorOperator
import datetime

from contrib.utils import check_time


class TimeSensor(BaseSensorOperator):
    def __init__(self, on_time, off_time, *args, **kwargs):
        super(TimeSensor, self).__init__(timeout=60 * 60 * 24 * 6, *args, **kwargs)
        self.on_time = on_time
        self.off_time = off_time

    def poke(self, context):
        current_time = datetime.datetime.now().time()
        print(current_time)
        return check_time(on_time=self.on_time, off_time=self.off_time, current_time=current_time)
