import calendar
import random
import re

import pandas as pd
from dateutil.relativedelta import relativedelta


def randomtimes(start, end, n):
    return [random.random() * (end - start) + start for _ in range(n)]


def randomtime(start, end):
    return random.random() * (end - start) + start


def random_start_end_time(start, end, max_timedelta=None):
    t1 = randomtime(start, end)
    if max_timedelta:
        t2 = randomtime(t1, t1 + max_timedelta)
    else:
        t2 = randomtime(t1, end)
    return [t1, t2]


def month_max_days(dt):
    return calendar.monthrange(dt.year, dt.month)[1]


def segmentation_cut_list(freq, start, end):
    step = 1
    re_ex = r'\d+\.?\d*'
    if re.match(re_ex, freq):
        step = int(re.findall(r"\d+\.?\d*", freq)[0])
    result = []
    if freq.endswith('Y'):
        [result.append(i) for i in range(start.year, end.year, step)]
        result.append(end.year)
    elif freq.endswith('M'):
        [result.append(i) for i in range(0, 12, step)]
        result.append(12)
    elif freq.endswith('D'):
        [result.append(i) for i in range(0, 31, step)]
        result.append(31)
    elif freq.endswith('W'):
        [result.append(i) for i in range(0, 7)]
    elif freq.endswith('H'):
        [result.append(i) for i in range(0, 24, step)]
        result.append(24)
    return result


def __split_start_end_time_to_list_year(start, end, freq):
    return [i.to_pydatetime().year for i in pd.date_range(start, end + relativedelta(years=1), freq=freq)]


def __split_start_end_time_to_list_month(start, end, freq):
    return [i.to_pydatetime().month for i in pd.date_range(start, end + relativedelta(months=1), freq=freq)]


def __split_start_end_time_to_list_day(start, end, freq):
    return [i.to_pydatetime().day for i in pd.date_range(start, end, freq=freq)]


def __split_start_end_time_to_list_week(start, end, freq):
    return [i.to_pydatetime().weekday() for i in pd.date_range(start, end, freq='D')]


def __split_start_end_time_to_list_hour(start, end, freq):
    return [i.to_pydatetime().hour for i in pd.date_range(start, end, freq=freq)]


def split_start_end_time_to_list(freq):
    '''freq is one of ['Y', 'M', 'D', 'W', 'H'],
    start , end is datetime'''
    if freq.endswith('Y'):
        return __split_start_end_time_to_list_year
    elif freq.endswith('M'):
        return __split_start_end_time_to_list_month
    elif freq.endswith('D'):
        return __split_start_end_time_to_list_day
    elif freq.endswith('W'):
        return __split_start_end_time_to_list_week
    elif freq.endswith('H'):
        return __split_start_end_time_to_list_hour


# def split_start_end_time_to_list(start, end, freq):
#     '''freq is one of ['Y', 'M', 'D', 'W', 'H'],
#     start , end is datetime'''
#     if freq.endswith('Y'):
#         return [i.to_pydatetime().year for i in pd.date_range(start, end + relativedelta(years=1), freq=freq)]
#     elif freq.endswith('M'):
#         return [i.to_pydatetime().month for i in pd.date_range(start, end + relativedelta(months=1), freq=freq)]
#     elif freq.endswith('D'):
#         return [i.to_pydatetime().day for i in pd.date_range(start, end, freq=freq)]
#     elif freq.endswith('W'):
#         return [i.to_pydatetime().weekday() for i in pd.date_range(start, end, freq='D')]
#     elif freq.endswith('H'):
#         return [i.to_pydatetime().hour for i in pd.date_range(start, end, freq=freq)]

# print(segmentation_cut_list('9H',datetime(2018,1,1),datetime(2020,1,1)))
# strrr = ''
# for i in pd.date_range('2019-01-01 00:20:24.008143', '2019-01-02 00:30:00', freq='H'):
#     strrr += '%s,' % (i.to_pydatetime().hour)
# print(strrr, strrr[:-1].split(','))


def __split_start_end_time_to_csv_year(start, end, freq):
    result = ''
    for i in pd.date_range(start, end + relativedelta(years=1), freq=freq):
        result += str(i.to_pydatetime().year) + ','
    return result[:-1]


def __split_start_end_time_to_csv_month(start, end, freq):
    result = ''
    for i in pd.date_range(start, end + relativedelta(months=1), freq=freq):
        result += str(i.to_pydatetime().month) + ','
    return result[:-1]


def __split_start_end_time_to_csv_day(start, end, freq):
    result = ''
    for i in pd.date_range(start, end, freq=freq):
        result += str(i.to_pydatetime().day) + ','
    return result[:-1]


def __split_start_end_time_to_csv_week(start, end, freq):
    result = ''
    for i in pd.date_range(start, end, freq='D'):
        result += str(i.to_pydatetime().weekday()) + ','
    return result[:-1]


def __split_start_end_time_to_csv_hour(start, end, freq):
    result = ''
    for i in pd.date_range(start, end, freq=freq):
        result += str(i.to_pydatetime().hour) + ','
    return result[:-1]


def split_start_end_time_to_csv(freq):
    '''freq is one of ['Y', 'M', 'D', 'W', 'H'],
    start , end is datetime'''
    if freq.endswith('Y'):
        return __split_start_end_time_to_csv_year
    elif freq.endswith('M'):
        return __split_start_end_time_to_csv_month
    elif freq.endswith('D'):
        return __split_start_end_time_to_csv_day
    elif freq.endswith('W'):
        return __split_start_end_time_to_csv_week
    elif freq.endswith('H'):
        return __split_start_end_time_to_csv_hour


def __split_start_end_time_to_range_list_year(start, end, freq, step):
    return [int(i.to_pydatetime().year / step) for i in pd.date_range(start, end + relativedelta(years=1), freq=freq)]


def __split_start_end_time_to_range_list_month(start, end, freq, step):
    return [int(i.to_pydatetime().month / step) for i in pd.date_range(start, end + relativedelta(months=1), freq=freq)]


def __split_start_end_time_to_range_list_day(start, end, freq, step):
    return [int(i.to_pydatetime().day / step) for i in pd.date_range(start, end, freq=freq)]


def __split_start_end_time_to_range_list_week(start, end, freq, step):
    return [int(i.to_pydatetime().weekday() / step) for i in pd.date_range(start, end, freq='D')]


def __split_start_end_time_to_range_list_hour(start, end, freq, step):
    return [int(i.to_pydatetime().hour / step) for i in pd.date_range(start, end, freq=freq)]


def split_start_end_time_to_range_list(freq):
    '''freq is one of ['Y', 'M', 'D', 'W', 'H'],
    start , end is datetime'''
    if freq.endswith('Y'):
        return __split_start_end_time_to_range_list_year
    elif freq.endswith('M'):
        return __split_start_end_time_to_range_list_month
    elif freq.endswith('D'):
        return __split_start_end_time_to_range_list_day
    elif freq.endswith('W'):
        return __split_start_end_time_to_range_list_week
    elif freq.endswith('H'):
        return __split_start_end_time_to_range_list_hour
