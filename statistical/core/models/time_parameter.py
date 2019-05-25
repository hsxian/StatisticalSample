from datetime import datetime

from dateutil.relativedelta import relativedelta


class TimeParameter(object):
    start = datetime.now() - relativedelta(months=1)
    end = datetime.now()
    segmentation = '3H'
    time_name = 'time'
    as_index = True

