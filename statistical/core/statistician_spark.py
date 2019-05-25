from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import ArrayType, IntegerType

from statistical.db.access.sports_data import SportsDao
from statistical.db.utils import str_to_list
from statistical.utils.time_util import split_start_end_time_to_range_list


class StatisticianOnSpark(object):
    def __init__(self):
        self.conf = SparkConf().setAppName("StatisticianOnSpark").setMaster("local[*]")
        self.sc = SparkContext.getOrCreate(self.conf)

        self.spark = SparkSession \
            .builder \
            .config(conf=self.conf) \
            .getOrCreate()

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

    def __filter_not_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = self.__get_selection_names(indexs, columns, None, None)
        not_pivot_items = set(filter_dic.keys()) - set(pivot_items)
        not_pivot_dic = {}
        for k, vs in filter_dic.items():
            if not_pivot_items.__contains__(k):
                not_pivot_dic[k] = F.array([F.lit(v) for v in vs])

        for k, vs in not_pivot_dic.items():
            df = df.filter(F.size(F.array_intersect(F.split(df[k], ",").cast("array<string>"), vs)) > 0)
        return df

    def __get_freq_step(self, freq):
        tag = freq[:-1]
        if not tag:
            tag = '1'
        return int(tag)

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = self.__get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []

            df = df.withColumn('start_time',
                               F.when(df.start_time < time_parm.start, time_parm.start).otherwise(df.start_time)) \
                .withColumn('end_time', F.when(df.end_time > time_parm.end, time_parm.end).otherwise(df.end_time))
            split_time_func = split_start_end_time_to_range_list(time_parm.segmentation)

            step = self.__get_freq_step(time_parm.segmentation)

            sss = F.udf(split_time_func, ArrayType(IntegerType()))
            df = df.withColumn(time_parm.time_name, F.explode(
                sss(F.col('start_time'), F.col('end_time'), F.lit(time_parm.segmentation), F.lit(step))))

        for x in split_items:
            df = df.withColumn(x, F.explode(F.split(x, ",")))
        return df

    def __transfer_filter_dic(self, filter_dic, sports_dic):
        result = {}
        for k, vs in filter_dic.items():
            ids = []
            for v in vs:
                ids.append(sports_dic[v])
            result[k] = ids
        return result

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

        rdd = self.__get_sports_data_rdd(indexs, columns, time_parm, filter_dic)
        df = rdd.toDF()

        df = self.process_data(df, indexs, columns, time_parm, filter_dic)

        df = self.__exec_statistician(df, indexs, columns, time_parm, SportsDao().sports_dict)

        return df

    def __filter_pivot_rows(self, df, indexs, columns, filter_dic):
        if not filter_dic:
            return df
        pivot_items = self.__get_selection_names(indexs, columns, None, None)
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
            filter_dic_id = self.__transfer_filter_dic(filter_dic, sports_dic)

        df = self.__filter_not_pivot_rows(df, indexs, columns, filter_dic_id)

        df = self.__split_pivot_rows(df, indexs, columns, time_parm)

        df = self.__filter_pivot_rows(df, indexs, columns, filter_dic_id)

        return df

    def __get_sports_data_rdd(self, indexs, columns, time_parm, filter_dic):
        selection_names = self.__get_selection_names(
            indexs, columns, time_parm, filter_dic)

        sports_records = SportsDao().get_sports_records(selection_names, time_parm.start, time_parm.end)
        data = list(sports_records.dicts())
        rdd = self.spark.sparkContext.parallelize(data)
        return rdd
