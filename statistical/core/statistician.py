import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

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

    def get_sports_records(self, indexs, columns, time_parm, filter_dic=None):
        indexs = self.__to_list(indexs)
        columns = self.__to_list(columns)

        selection_names = self.__get_selection_names(
            indexs, columns, time_parm, filter_dic)

        selections = self.__get_selections(selection_names)

        sports_records = SportsRecord.select(*selections) \
            .where(SportsRecord.start_time < time_parm.end) \
            .where(SportsRecord.end_time > time_parm.start)

        return list(sports_records.dicts())

    def sports_record_statistics(self, sports_records, indexs, columns, time_parm, filter_dic=None):
        indexs = self.__to_list(indexs)
        columns = self.__to_list(columns)

        df = self.__exec_statistician(
            sports_records, indexs, columns, time_parm, filter_dic)

        return df

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

        if time_parm and time_parm.segmentation:
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

    def __transfer_filter_dic(self, filter_dic):
        result = {}
        dic = SportsDao().sports_dict
        for k, vs in filter_dic.items():
            ids = []
            for v in vs:
                ids.append(dic[v])
            result[k] = ids
        return result

    def __filter_not_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = self.__get_selection_names(indexs, columns, None, None)
        not_pivot_items = set(filter_dic.keys()) - set(pivot_items)
        not_pivot_dic = {}
        for k, vs in filter_dic.items():
            if not_pivot_items.__contains__(k):
                not_pivot_dic[k] = vs

        for k, vs in not_pivot_dic.items():
            df = df[df.apply(lambda x: len(set(x[k]) & set(vs)) > 0, axis=1)]
        return df

    def __transfer_csv_to_list(self, df, indexs, columns, filter_dic):
        csv_to_list_items = self.__get_selection_names(
            indexs, columns, None, filter_dic)
        df_csv_to_lst(df, csv_to_list_items)

    def __filter_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = self.__get_selection_names(indexs, columns, None, None)
        for k, vs in filter_dic.items():
            if pivot_items.__contains__(k):
                df = df[df[k].isin(vs)]
        return df

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = self.__get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []
            for index, row in df.iterrows():
                start, end = self.__determining_time_boundaries(time_parm, row)
                time_rows.append(split_start_end_time_to_list(
                    start, end, time_parm.segmentation))
            df[time_parm.time_name] = time_rows
            df = df.drop(columns=['start_time', 'end_time'])
            split_items.append(time_parm.time_name)

        for item in split_items:
            df = split_data_frame_list(df, item)
        return df

    def __get_time_cut(self, df, time_parm):
        if time_parm.segmentation:
            cut_list = segmentation_cut_list(
                time_parm.segmentation, time_parm.start, time_parm.end)
            time_cut = pd.cut(df[time_parm.time_name], cut_list, right=False)
            return time_cut

    def __exec_statistician(self, sports_records, indexs, columns, time_parm, filter_dic=None):
        if not sports_records:
            return

        filter_dic_id = filter_dic
        if filter_dic:
            filter_dic_id = self.__transfer_filter_dic(filter_dic)

        df = pd.DataFrame(sports_records)
        # print('-+' * 20)
        # print(df)

        self.__transfer_csv_to_list(df, indexs, columns, filter_dic_id)
        # print('-+' * 20, 'transfer_csv_to_list')
        # print(df)

        df = self.__filter_not_pivot_rows(df, indexs, columns, filter_dic_id)
        # print('-+' * 20, 'filter_not_pivot_rows')
        # print(df)

        df = self.__split_pivot_rows(df, indexs, columns, time_parm)
        # print('-+' * 20, 'split_pivot_rows')
        # print(df)

        df = self.__filter_pivot_rows(df, indexs, columns, filter_dic_id)
        # print('-+' * 20, 'filter_pivot_rows')
        # print(df)

        time_cut = self.__get_time_cut(df, time_parm)
        if time_parm.segmentation and time_parm.as_index:
            indexs.append(time_cut)
        elif time_parm.segmentation:
            columns.append(time_cut)

        df['count'] = 1
        df = df.pivot_table('count', index=indexs,
                            columns=columns, aggfunc='sum')
        df.dropna(axis=0, how='all', inplace=True)
        df = df.fillna(0)

        df = df.rename(columns=SportsDao().sports_dict, index=SportsDao().sports_dict)
        # print('-+' * 20)
        # print(df)

        return df
