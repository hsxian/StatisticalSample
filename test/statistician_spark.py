from datetime import datetime

from statistical.core.models.time_parameter import TimeParameter
from statistical.core.statistician_spark import StatisticianOnSpark


def tatistician_on_spark_test():
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
    df = sos.sports_record_statistics(
        indexs=indexs,
        columns=columns,
        time_parm=time,
        filter_dic=filter_dic
    )
    df.show(1000)


now = datetime.now()
tatistician_on_spark_test()
print('tatistician_test', datetime.now() - now, '-+' * 20)
