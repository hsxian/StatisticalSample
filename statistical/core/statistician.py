import datetime
from concurrent.futures import ProcessPoolExecutor
# from multiprocessing.dummy import  Pool#线程
from multiprocessing import Pool  # 进程
from multiprocessing import cpu_count

import numpy as np
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
        sports_dic = SportsDao().sports_dict

        # cpu_ct = cpu_count()
        cpu_ct = 2
        p = Pool(cpu_ct)  # 创建4个进程
        data_ct = len(sports_records)
        per_ct = int(data_ct / cpu_ct)
        par_data = [sports_records[i:i + per_ct] for i in range(0, data_ct, per_ct)]
        dfs_pool = []
        for cpu in range(len(par_data)):
            args = (par_data[cpu], indexs, columns, time_parm, sports_dic, filter_dic)
            d = p.apply_async(self.process_data, args=args)
            dfs_pool.append(d)
        print('Waiting for all subprocesses done...')
        p.close()
        p.join()
        print('All subprocesses done.')

        dfs = []
        for df_pool in dfs_pool:
            d = df_pool.get()
            if not d.empty:
                dfs.append(d)
        df = pd.concat(dfs, ignore_index=True)
        df = self.__exec_statistician(df, indexs, columns, time_parm, sports_dic)

        return df

    def sports_record_statistics2(self, sports_records, indexs, columns, time_parm, filter_dic=None):
        indexs = self.__to_list(indexs)
        columns = self.__to_list(columns)
        sports_dic = SportsDao().sports_dict

        cpu_ct = cpu_count()
        p = Pool(cpu_ct)
        data_ct = len(sports_records)
        per_ct = int(data_ct / cpu_ct)
        par_data = [sports_records[i:i + per_ct] for i in range(0, data_ct, per_ct)]
        dfs_pool = []
        dfs = []
        with ProcessPoolExecutor() as executor:
            for cpu in range(len(par_data)):
                args = (par_data[cpu], indexs, columns, time_parm, sports_dic, filter_dic)
                d = executor.submit(self.process_data, *args)
                dfs_pool.append(d)
        for d in dfs_pool:
            d = d.result()
            if not d.empty:
                dfs.append(d)

        df = pd.concat(dfs, ignore_index=True)
        df = self.__exec_statistician(df, indexs, columns, time_parm, sports_dic)

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

        result = []
        for i in selections:
            si = str(i)
            if result.__contains__(si):
                continue
            result.append(si)
        return result
        # return list(set(selections))  # distinct

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

    def __transfer_filter_dic(self, filter_dic, sports_dic):
        result = {}
        for k, vs in filter_dic.items():
            ids = []
            for v in vs:
                ids.append(sports_dic[v])
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
            if pivot_items.__contains__(k) and k in df.columns:
                df = df[df[k].isin(vs)]
        return df

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = self.__get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []
            df = df.assign(start_time=np.maximum(df.start_time, np.datetime64(time_parm.start)),
                           end_time=np.minimum(df.end_time, np.datetime64(time_parm.end)))

            for index, row in df.iterrows():
                time_rows.append(split_start_end_time_to_list(
                    row['start_time'],
                    row['end_time'],
                    time_parm.segmentation))

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

    def process_data(self, sports_records, indexs, columns, time_parm, sports_dic, filter_dic=None):
        if not sports_records:
            return
        # now = datetime.datetime.now()
        filter_dic_id = filter_dic
        if filter_dic:
            filter_dic_id = self.__transfer_filter_dic(filter_dic, sports_dic)

        df = pd.DataFrame(sports_records)

        # print('-+' * 20,'DataFrame',datetime.datetime.now()-now)
        # now = datetime.datetime.now()
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

        return df

    def __exec_statistician(self, df, indexs, columns, time_parm, sports_dic):
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

        df = df.rename(columns=sports_dic, index=sports_dic)
        # print('-+' * 20)
        # print(df)

        return df
