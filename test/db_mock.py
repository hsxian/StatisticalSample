import pandas as pd

from statistical.conf.database_conf import db
from statistical.db.access.sports_data import SportsDao
from statistical.db.mock.sports_record import SportsRecordMocker
from statistical.db.models.sports import SportsRecord


def mock_data(start_time, end_tiem, size):
    with db.execution_context():
        srm = SportsRecordMocker()
        srm.init_table()
        srm.mock_dic_cgy()
        srm.mock_dic()
        srm.mock_person()
        srm.mock_sports_record(start_time, end_tiem, size)


def print_sports_record(size):
    with db.execution_context():
        dic_data = SportsDao().sports_dict
        record_data = SportsRecord.select().limit(size)
        df = pd.DataFrame(list(record_data.dicts()))
        print(df)
        print('-+' * 20)

# mock_data(datetime.datetime(2019, 1, 1), datetime.datetime(2019, 6, 1), 10000)
# print_sports_record(20000)
