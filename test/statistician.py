from datetime import datetime

from statistical.conf.database_conf import db
from statistical.core.statistician import SportsRecordStatistician, TimeParameter


def tatistician_test():
    with db.execution_context():
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
        srs = SportsRecordStatistician()
        data = srs.get_sports_records(
            indexs=indexs,
            columns=columns,
            time_parm=time,
            filter_dic=filter_dic
        )

        print('data count:', len(data))
        # print(pandas.DataFrame(data))

        df = SportsRecordStatistician().sports_record_statistics(
            sports_records=data,
            indexs=indexs,
            columns=columns,
            time_parm=time,
            filter_dic=filter_dic
        )
        print(df)


now = datetime.now()
tatistician_test()
print('tatistician_test', datetime.now() - now, '-+' * 20)
