from datetime import datetime

import pandas as pd
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import split, explode, col
from pyspark.sql.session import SparkSession

from statistical.core.statistician import TimeParameter
from statistical.db.access.sports_data import SportsDao
from statistical.db.utils import str_to_list
from statistical.utils.time_util import split_start_end_time_to_list


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

    def __split_pivot_rows(self, df, indexs, columns, time_parm):
        split_items = self.__get_selection_names(indexs, columns, None, None)
        if time_parm.segmentation:
            time_rows = []
            # df = df.assign(start_time=np.maximum(df.start_time, np.datetime64(time_parm.start)),
            #                end_time=np.minimum(df.end_time, np.datetime64(time_parm.end)))

            split_time_func = split_start_end_time_to_list(time_parm.segmentation)

            for index, row in df.iterrows():
                time_rows.append(split_time_func(
                    row['start_time'],
                    row['end_time'],
                    time_parm.segmentation))


        df1 = df.drop(columns=split_items, axis=1)
        for x in split_items:
            df1 = df1.join(df[x].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename(x))
        df = df1.reset_index(drop=True)

        return df
    def sports_record_statistics(self, indexs, columns, time_parm, filter_dic=None):
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)

        rdd = self.__get_sports_data_rdd(indexs, columns, time_parm, filter_dic)
        df = rdd.toDF()
        df = df.withColumn("item", explode(split("item", ",")))
        # df = df.withColumn("time", pd.date_range(col("start_time"), col("end_time")))

        df.show()

    def __get_sports_data_rdd(self, indexs, columns, time_parm, filter_dic):
        selection_names = self.__get_selection_names(
            indexs, columns, time_parm, filter_dic)

        sports_records = SportsDao().get_sports_records(selection_names, time_parm.start, time_parm.end)
        data = list(sports_records.dicts())
        rdd = self.spark.sparkContext.parallelize(data)
        return rdd


indexs = None

columns = 'item'

time = TimeParameter()
time.segmentation = 'H'
# time.as_index=False
time.start = datetime(2019, 1, 1)
time.end = datetime(2019, 1, 1, 0, 30)

filter_dic = {
    'equipment': ['bicycle'],  # 7
    'item': ['swim', 'riding', 'gaming']  # 10,11,12
}

sos = StatisticianOnSpark()
sos.sports_record_statistics(
    indexs=indexs,
    columns=columns,
    time_parm=time,
    filter_dic=filter_dic
)
