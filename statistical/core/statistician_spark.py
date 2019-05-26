from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, IntegerType

from statistical.core.utils import get_selection_names, transfer_filter_dic
from statistical.db.access.sports_data import SportsDao
from statistical.db.utils import str_to_list
from statistical.utils.stopwatch import StopWatch
from statistical.utils.time_util import split_start_end_time_to_range_list


class StatisticianOnSpark(object):
    def __init__(self):
        self.conf = SparkConf().setAppName("StatisticianOnSpark").setMaster("local[*]")
        self.spark = SparkSession \
            .builder \
            .config(conf=self.conf) \
            .getOrCreate()

    def __filter_not_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = get_selection_names(indexs, columns, None, None)
        not_pivot_items = set(filter_dic.keys()) - set(pivot_items)
        not_pivot_dic = {}
        for k, vs in filter_dic.items():
            if not_pivot_items.__contains__(k):
                not_pivot_dic[k] = F.array([F.lit(v) for v in vs])

        for k, vs in not_pivot_dic.items():
            df = df.filter(F.size(F.array_intersect(F.split(df[k], ",").cast("array<string>"), vs)) > 0).drop(k)
        return df

    def __get_freq_step(self, freq):
        tag = freq[:-1]
        if not tag:
            tag = '1'
        return int(tag)

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []

            df = df.withColumn('start_time',
                               F.when(df.start_time < time_parm.start, time_parm.start).otherwise(df.start_time)) \
                .withColumn('end_time',
                            F.when(df.end_time > time_parm.end, time_parm.end).otherwise(df.end_time))

            split_time_func = split_start_end_time_to_range_list(time_parm.segmentation)

            step = self.__get_freq_step(time_parm.segmentation)

            sss = F.udf(split_time_func, ArrayType(IntegerType()))

            df = df.withColumn(time_parm.time_name,
                               F.explode(
                                   sss(F.col('start_time'),
                                       F.col('end_time'),
                                       F.lit(time_parm.segmentation),
                                       F.lit(step)))) \
                .drop('start_time', 'end_time')

        for x in split_items:
            df = df.withColumn(x, F.explode(F.split(x, ",")))
        return df

    def __exec_statistician(self, df, indexs, columns, time_parm, sports_dic):
        pivot_items = []
        pivot_items.extend(indexs)
        pivot_items.extend(columns)

        if time_parm.segmentation:
            pivot_items.insert(0, time_parm.time_name)

        df = df.withColumn('count', F.lit(1))
        df = df.groupBy(*pivot_items).agg(F.sum('count'))

        if time_parm.segmentation:
            pivot_items.remove(time_parm.time_name)

        sss = F.udf(lambda x: sports_dic[x])
        for item in pivot_items:
            df = df.withColumn(item, sss(F.col(item)))
        if time_parm.segmentation:
            df = df.sort(time_parm.time_name)

        return df

    def sports_record_statistics(self, indexs, columns, time_parm, filter_dic=None):
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)

        sth = StopWatch()
        sth.start()

        rdd = self.__get_sports_data_rdd(indexs, columns, time_parm, filter_dic)
        df = rdd.toDF()
        print('__get_sports_data_rdd to df:', sth.elapsed, '-+' * 20)
        sth.start()

        df = self.process_data(df, indexs, columns, time_parm, filter_dic)

        sth.start()
        df = self.__exec_statistician(df, indexs, columns, time_parm, SportsDao().sports_dict)
        print('__exec_statistician to df:', sth.elapsed, '-+' * 20)

        return df

    def __filter_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = get_selection_names(indexs, columns, None, None)
        for k, vs in filter_dic.items():
            if pivot_items.__contains__(k) and k in df.columns:
                df = df.filter(df[k].isin(vs))

        return df

    def process_data(self, df, indexs, columns, time_parm, filter_dic=None):
        if not df:
            return

        filter_dic_id = filter_dic
        if filter_dic:
            sports_dic = SportsDao().sports_dict
            filter_dic_id = transfer_filter_dic(filter_dic, sports_dic)

        sth = StopWatch()
        sth.start()

        df = self.__filter_not_pivot_rows(df, indexs, columns, filter_dic_id)
        print('__filter_not_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()

        df = self.__split_pivot_rows(df, indexs, columns, time_parm)
        print('__split_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()

        df = self.__filter_pivot_rows(df, indexs, columns, filter_dic_id)
        print('__filter_pivot_rows:', sth.elapsed, '-+' * 20)
        sth.start()

        return df

    def __get_sports_data_rdd(self, indexs, columns, time_parm, filter_dic):
        selection_names = get_selection_names(
            indexs, columns, time_parm, filter_dic)

        sports_records = SportsDao().get_sports_records(selection_names, time_parm.start, time_parm.end)
        data = list(sports_records.dicts())
        rdd = self.spark.sparkContext.parallelize(data)
        return rdd
