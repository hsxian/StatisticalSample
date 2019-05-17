from statistical.db.mock.sports_record import SportsRecordMocker
from statistical.conf.database_conf import db

with db.execution_context():
    srm = SportsRecordMocker()
    # srm.init_table()
    srm.mock_dic_cgy()
    srm.mock_dic()
    srm.mock_person()
    srm.mock_sports_record(10000)
    srm.print_sports_record(10)


