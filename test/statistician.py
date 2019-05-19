from datetime import datetime

import pandas

from statistical.conf.database_conf import db
from statistical.core.statistician import SportsRecordStatistician, Timeparameter


def tatistician_test():
    with db.execution_context():

        indexs = None

        columns = 'item'

        time = Timeparameter()
        time.segmentation = 'H'
        # time.as_index=False
        time.start = datetime(2019, 1, 1)
        time.end = datetime(2019, 1, 1, 1, 30)

        filter_dic = {
            'equipment': ['bicycle'],
            'item': ['swim', 'riding', 'gaming']
        }
        
        srs = SportsRecordStatistician()
        data = srs.get_sports_records(
            indexs=indexs,
            columns=columns,
            time_parm=time,
            filter_dic=filter_dic
        )

        print(pandas.DataFrame(data))

        df = SportsRecordStatistician().sports_record_statistics(
            sports_records=data,
            indexs=indexs,
            columns=columns,
            time_parm=time,
            filter_dic=filter_dic
        )
        print(df)


tatistician_test()
