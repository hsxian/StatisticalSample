from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import calendar
import datetime
import random


def randomtimes(start, end, n):
    return [random.random() * (end - start) + start for _ in range(n)]


def randomtime(start, end):
    return random.random() * (end - start) + start


def random_start_end_time(start, end, max_timedelta=None):
    t1 = randomtime(start, end)
    if max_timedelta:
        t2 = randomtime(t1, t1+max_timedelta)
    else:
        t2 = randomtime(t1, end)
    return [t1, t2]



def month_max_days(dt):
    return calendar.monthrange(dt.year, dt.month)[1]


def split_start_end_time_to_list(start, end, fmt):
    '''fmt is one of ['Y', 'M', 'D', 'W', 'H'],
    start , end is datetime'''
    if 'Y' == fmt:
        return [i.to_pydatetime().year for i in pd.date_range(start, end + relativedelta(years=1), freq='Y')]
    elif 'M' == fmt:
        return [i.to_pydatetime().month for i in pd.date_range(start, end + relativedelta(months=1), freq='M')]
    elif 'D' == fmt:
        return [i.to_pydatetime().day for i in pd.date_range(start, end, freq='D')]
    elif 'W' == fmt:
        return [i.to_pydatetime().weekday() for i in pd.date_range(start, end, freq='D')]
    elif 'H' == fmt:
        return [i.to_pydatetime().hour for i in pd.date_range(start, end, freq='H')]
