from statistical.db.mock.sports_record import SportsRecordMocker
from statistical.conf.logger_conf import logger
from statistical.utils.pandas_util import split_data_frame_list
from statistical.conf.database_conf import db
from statistical.db.models.sports import *
from statistical.utils.time_util import *
from statistical.utils.linq import Linq
from statistical.db.utils import *
import os
import sys
import numpy as np
import datetime
import random
import pandas as pd

sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录


def mock_data():
    with db.execution_context():
        srm = SportsRecordMocker()
        # srm.init_table()
        srm.mock_dic_cgy()
        srm.mock_dic()
        srm.mock_person()
        srm.mock_sports_record(10000)


def print_sports_record(size):
    with db.execution_context():
        dic_data = pw_lst_2_py_dic(list(Dictionary.select()))
        record_data = SportsRecord.select().limit(size)
        df = pd.DataFrame(list(record_data.dicts()))
        # print(df)
        # print('-+'*20)
        df['time'] = [split_start_end_time_to_list(
            row['start_time'], row['end_time'], '4H')for index, row in df.iterrows()]

        df_csv_to_lst(df, ['site', 'equipment', 'item'])

        # df = split_data_frame_list(df, 'site')
        # df = split_data_frame_list(df, 'equipment')
        df = split_data_frame_list(df, 'item')
        df = split_data_frame_list(df, 'time')
        # df_cols_id_2_name(df, ['site', 'equipment', 'item'], dic_data)
        df_cols_id_2_name(df, ['item'], dic_data)
        df['count'] = 1
        df = df.drop(columns=['id', 'start_time', 'end_time',
                              'site', 'equipment', 'person'])
        # print(df)
        # print('-+' * 20)

        time = pd.cut(df['time'], [0, 4, 8, 12, 16, 20, 24], right=False)
        df = df.pivot_table('count', index=time, columns='item', aggfunc='sum')
        df.dropna(axis=0, how='all', inplace=True)
        print(df)

print_sports_record(20000)
