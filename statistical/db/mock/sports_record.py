import os
import sys
import numpy as np
import datetime
import random
import pandas as pd

sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录

from statistical.conf.logger_conf import logger
from statistical.utils.pandas_util import split_data_frame_list
from statistical.conf.database_conf import db
from statistical.db.models.sports import *
from statistical.utils.time_util import random_start_end_time
from statistical.utils.linq import Linq
from statistical.db.utils import *

class SportsRecordMocker:

    dic_cgy_datas = [
        {'name': 'site'},  # 场地
        {'name': 'equipment'},  # 设备
        {'name': 'item'},  # 项目
    ]

    dic_datas = [
        {'category': 1, 'name': 'gym', 'remarks': ''},
        {'category': 1, 'name': 'swimming pool', 'remarks': ''},
        {'category': 1, 'name': 'highway', 'remarks': ''},
        {'category': 1, 'name': 'e-sports room', 'remarks': ''},

        {'category': 2, 'name': 'basketball', 'remarks': ''},
        {'category': 2, 'name': 'swimsuit', 'remarks': ''},
        {'category': 2, 'name': 'bicycle', 'remarks': ''},
        {'category': 2, 'name': 'computer', 'remarks': ''},

        {'category': 3, 'name': 'basketball', 'remarks': ''},
        {'category': 3, 'name': 'swim', 'remarks': ''},
        {'category': 3, 'name': 'riding', 'remarks': ''},
        {'category': 3, 'name': 'gaming', 'remarks': ''},
    ]

    person_datas = [
        {'name': 'Yang', 'age': 26},
        {'name': 'Judy', 'age': 21},
        {'name': 'Joshua', 'age': 29},
        {'name': 'Edward', 'age': 20}
    ]

    def init_table(self):
        with db.execution_context():
            tabs = [Person, DictionaryCategory, Dictionary, SportsRecord]
            dbtabs = db.get_tables()
            if (dbtabs):
                db.drop_tables(tabs)
                logger.info('drop tables: {}'.format(dbtabs))
            db.create_tables(tabs)
            logger.info('createname tables: {}'.format(tabs))

    def mock_dic_cgy(self):
        with db.atomic():
            cot = DictionaryCategory.select().count()
            if cot == 0:
                # logger.info(self.dic_cgy_datas)
                DictionaryCategory.insert_many(self.dic_cgy_datas).execute()
                logger.info('insert {} item to dictionarycategory.'.format(
                    len(self.dic_cgy_datas)))

    def mock_dic(self):
        with db.atomic():
            cot = Dictionary.select().count()
            if cot == 0:
                # logger.info(self.dic_datas)
                Dictionary.insert_many(self.dic_datas).execute()
                logger.info('insert {} item to dictionary.'.format(
                    len(self.dic_datas)))

    def mock_person(self):
        with db.atomic():
            cot = Person.select().count()
            if cot == 0:
                # logger.info(self.dic_datas)
                Person.insert_many(self.person_datas).execute()
                logger.info('insert {} item to person.'.format(
                    len(self.person_datas)))

    def __get_dic_ids(self, dic_data, dic_cgy_data, category_name):
        dic_cay_id = Linq(dic_cgy_data).first(
            lambda x: x.name == category_name).id

        dic_ids = Linq(dic_data).where(lambda x: x.category.id ==
                                        dic_cay_id).select(lambda x: str(x.id)).to_list()

        return dic_ids

    def __random_dic_ids(self, dic_data, dic_cgy_data,  category_name):
        dic_ids = self.__get_dic_ids(dic_data, dic_cgy_data, category_name)
        cot = len(dic_ids)
        dic_ids = np.random.choice(
            dic_ids, random.randint(0, cot), replace=False)

        return ','.join(dic_ids)

    def __mock_sports_record_item(self, dic_data, dic_cgy_data,):
        se = random_start_end_time(datetime.datetime(
            2019, 1, 1), datetime.datetime(2019, 6, 1), datetime.timedelta(days=1))

        return {
            'start_time': se[0],
            'end_time': se[1],
            'site': self.__random_dic_ids(dic_data, dic_cgy_data, 'site'),
            'equipment': self.__random_dic_ids(dic_data, dic_cgy_data, 'equipment'),
            'item': self.__random_dic_ids(dic_data, dic_cgy_data, 'item'),
            'person': random.randint(1, Person.select().count()),
        }

    def mock_sports_record(self, size):
        with db.atomic():
            cot = SportsRecord.select().count()
            if cot == 0:
                count = int(size/1000)
                dic_data = list(Dictionary.select())
                dic_cgy_data = list(DictionaryCategory.select())

                for i in range(count):
                    sports_record_datas = [
                        self.__mock_sports_record_item(dic_data, dic_cgy_data) for j in range(1000)]
                    # logger.info(sports_record_datas)
                    SportsRecord.insert_many(sports_record_datas).execute()
                    logger.info('insert {} item to sports_record.'.format(
                        len(sports_record_datas)))


    def print_sports_record(self, size):
        with db.execution_context():
            dic_data = pw_lst_2_py_dic(list(Dictionary.select()))
            record_data = SportsRecord.select().limit(size)
            df = pd.DataFrame(list(record_data.dicts()))       
            df_csv_to_lst(df, ['site', 'equipment', 'item'])
            df = split_data_frame_list(df, 'site')
            df = split_data_frame_list(df, 'equipment')
            df = split_data_frame_list(df, 'item')
            df_cols_id_2_name(df, ['site', 'equipment', 'item'], dic_data)
            print(df.head())


if not db.is_closed():
    db.close()
