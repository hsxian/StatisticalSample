from datetime import datetime

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession

from statistical.core.statistician import TimeParameter
from statistical.db.access.sports_data import SportsDao
from statistical.db.utils import str_to_list
from statistical.utils.stopwatch import StopWatch
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
            sth = StopWatch()
            sth.start()
            df = df.withColumn('start_time', F.when(df.start_time < time_parm.start, time_parm.start).otherwise(df.start_time))
            df = df.withColumn('end_time', F.when(df.end_time > time_parm.end, time_parm.end).otherwise(df.end_time))
            split_time_func = split_start_end_time_to_list(time_parm.segmentation)

            print('.when(df.start_time',sth.elapsed)
            # for row in df.collect():
                # row=row.asDict()
                # row['start_time']=max(row['start_time'],time_parm.start)
                # row['end_time']=min(row['end_time'],time_parm.end)
                # print(row)
        #     for index, row in df.iterrows():
        #         time_rows.append(split_time_func(
        #             row['start_time'],
        #             row['end_time'],
        #             time_parm.segmentation))
        #
        #
        # df1 = df.drop(columns=split_items, axis=1)
        # for x in split_items:
        #     df1 = df1.join(df[x].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename(x))
        # df = df1.reset_index(drop=True)

        return df

    def sports_record_statistics(self, indexs, columns, time_parm, filter_dic=None):
        indexs = str_to_list(indexs)
        columns = str_to_list(columns)

        rdd = self.__get_sports_data_rdd(indexs, columns, time_parm, filter_dic)
        df = rdd.toDF()
        df = df.withColumn("item", F.explode(F.split("item", ",")))
        # df = df.withColumn("time", pd.date_range(col("start_time"), col("end_time")))
        self.__split_pivot_rows(df, indexs, columns, time_parm)
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
time.end = datetime(2019, 6, 1, 0, 30)

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
