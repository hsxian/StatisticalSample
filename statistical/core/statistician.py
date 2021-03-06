from concurrent.futures import ProcessPoolExecutor
# from multiprocessing.dummy import  Pool#线程
from multiprocessing import Pool  # 进程
from multiprocessing import cpu_count

import numpy as np
import pandas as pd

from statistical.core.utils import get_selection_names, transfer_filter_dic
from statistical.db.access.sports_data import SportsDao
from statistical.db.utils import df_csv_to_lst, str_to_list
from statistical.utils.stopwatch import StopWatch
from statistical.utils.time_util import segmentation_cut_list, split_start_end_time_to_csv


class SportsRecordStatistician(object):

    def get_sports_records(self, indexs, columns, time_parm, filter_dic=None):
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)

        selection_names = get_selection_names(
            indexs, columns, time_parm, filter_dic)

        sports_records = SportsDao().get_sports_records(selection_names, time_parm.start, time_parm.end)

        return list(sports_records.dicts())

    def sports_record_statistics(self, sports_records, indexs, columns, time_parm, filter_dic=None):
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)
        sports_dic = SportsDao().sports_dict

        cpu_ct = cpu_count()
        # cpu_ct = 1
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
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)
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

    def __filter_not_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = get_selection_names(indexs, columns, None, None)
        not_pivot_items = set(filter_dic.keys()) - set(pivot_items)
        not_pivot_dic = {}
        for k, vs in filter_dic.items():
            if not_pivot_items.__contains__(k):
                not_pivot_dic[k] = vs

        for k, vs in not_pivot_dic.items():
            df = df[df.apply(lambda x: len(set(x[k]) & set(vs)) > 0, axis=1)]
        return df

    def __filter_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = get_selection_names(indexs, columns, None, None)
        for k, vs in filter_dic.items():
            if pivot_items.__contains__(k) and k in df.columns:
                df = df[df[k].isin(vs)]
        return df

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []
            df = df.assign(start_time=np.maximum(df.start_time, np.datetime64(time_parm.start)),
                           end_time=np.minimum(df.end_time, np.datetime64(time_parm.end)))

            split_time_csv_func = split_start_end_time_to_csv(time_parm.segmentation)

            df[time_parm.time_name] = df.apply(
                lambda x: split_time_csv_func(x['start_time'], x['end_time'], time_parm.segmentation), axis=1)

            df = df.drop(columns=['start_time', 'end_time'])

            split_items.append(time_parm.time_name)

        df1 = df.drop(columns=split_items, axis=1)
        for x in split_items:
            df1 = df1.join(df[x].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename(x))
            df1[x] = df1[x].astype(np.str)

        df = df1.reset_index(drop=True)

        if time_parm.segmentation:
            df[time_parm.time_name] = df[time_parm.time_name].astype(np.int16)
        print(df.info())
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

        sth = StopWatch()
        sth.start()

        filter_dic_id = filter_dic
        if filter_dic:
            filter_dic_id = transfer_filter_dic(filter_dic, sports_dic)

        df = pd.DataFrame(sports_records)

        print('DataFrame:', sth.elapsed, '-+' * 20)
        # print(df)

        df = self.__filter_not_pivot_rows(df, indexs, columns, filter_dic_id)
        print('__filter_not_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()
        # print(df)

        df = self.__split_pivot_rows(df, indexs, columns, time_parm)
        print('__split_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()
        # print(df)

        df = self.__filter_pivot_rows(df, indexs, columns, filter_dic_id)
        print('__filter_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()
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
