import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from statistical.conf.database_conf import db
from statistical.db.access.sports_data import SportsDao
from statistical.db.models.sports import SportsRecord
from statistical.db.utils import df_csv_to_lst
from statistical.utils.pandas_util import split_data_frame_list
from statistical.utils.time_util import split_start_end_time_to_list, segmentation_cut_list


class Timeparameter(object):
    start = datetime.datetime.now() - relativedelta(months=1)
    end = datetime.datetime.now()
    segmentation = '3H'
    time_name = 'time'
    as_index = True


class SportsRecordStatistician(object):

    def sports_record_statistics(self, indexs, columns, time_parm, filter_dic=None):

        indexs = self.__to_list(indexs)
        columns = self.__to_list(columns)

        selection_names = self.__get_selection_names(indexs, columns, time_parm, filter_dic)
        selections = self.__get_selections(selection_names)
        sports_records = SportsRecord.select(*selections) \
            .where(SportsRecord.start_time < time_parm.end) \
            .where(SportsRecord.end_time > time_parm.start)
        self.__exec_statistician(sports_records, indexs, columns, time_parm, filter_dic)
        return sports_records

    def __to_list(self, items):
        result = []
        if isinstance(items, str):
            result.append(items)
        elif isinstance(items, list):
            result.extend(items)
        return result

    def __get_selection_names(self, indexs, columns, time_parm, filter_dic):
        selections = []
        selections.extend(indexs)
        selections.extend(columns)

        if time_parm.segmentation:
            selections.extend(['start_time', 'end_time'])

        if filter_dic:
            selections.extend(filter_dic.keys())

        return list(set(selections))  # distinct

    def __get_class_pro(self, name):
        if hasattr(SportsRecord, name):
            return getattr(SportsRecord, name)

    def __get_selections(self, selection_names):
        querys = []
        if isinstance(selection_names, str):
            query = self.__get_class_pro(selection_names)
            if query:
                querys.append(query)
        elif isinstance(selection_names, list):
            for name in selection_names:
                query = self.__get_class_pro(name)
                if query:
                    querys.append(query)
        return querys

    def __determining_time_boundaries(self, time_parm, df_row):
        start, end = df_row['start_time'], df_row['end_time']
        if start < time_parm.start:
            start = time_parm.start
        if end > time_parm.end:
            end = time_parm.end
        return start, end

    def __exec_statistician(self, sports_records, indexs, columns, time_parm, filter_dic=None):
        if not sports_records:
            return
        df = pd.DataFrame(list(sports_records.dicts()))
        print('-+' * 20)
        # print(df)
        split_items = []
        split_items.extend(indexs)
        split_items.extend(columns)

        df_csv_to_lst(df, split_items)

        if time_parm.segmentation:
            time_rows = []
            for index, row in df.iterrows():
                start, end = self.__determining_time_boundaries(time_parm, row)
                time_rows.append(split_start_end_time_to_list(start, end, time_parm.segmentation))
            df[time_parm.time_name] = time_rows
            df = df.drop(columns=['start_time', 'end_time'])
            split_items.append(time_parm.time_name)

        for item in split_items:
            df = split_data_frame_list(df, item)

        if time_parm.segmentation:
            cut_list = segmentation_cut_list(time_parm.segmentation, time_parm.start, time_parm.end)
            time_cut = pd.cut(df[time_parm.time_name], cut_list, right=False)
            if time_parm.as_index:
                indexs.append(time_cut)
            else:
                columns.append(time_cut)

        df['count'] = 1
        df = df.pivot_table('count', index=indexs, columns=columns, aggfunc='sum')
        df.dropna(axis=0, how='all', inplace=True)
        df = df.fillna(0)

        df = df.rename(columns=SportsDao().sports_dict,index=SportsDao().sports_dict)
        print('-+' * 20)
        print(df)


with db.execution_context():
    time = Timeparameter()
    # time.segmentation = 'H'
    # time.as_index=False
    time.start = datetime.datetime(2019, 1, 1)
    time.end = datetime.datetime(2019, 6, 1, 1, 30)

    ss = SportsRecordStatistician().sports_record_statistics( \
        indexs=['site'], \
        columns='item', \
        time_parm=time, \
        filter_dic={ \
            'equipment': ['qwe'], \
            })

    # [print(model_to_dict(s)) for s in ss]
