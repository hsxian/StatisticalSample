import os
import sys
import numpy as np
import datetime
import random
import pandas as pd

sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录

from Conf.LoggerConf import logger
from Utils.DictionaryUtil import *
from Utils.PandasUtil import split_data_frame_list
from Conf.DataBaseConf import db
from Models.Sports import *
from Utils.TimeUtil import random_start_end_time
from Utils.Linq import Linq

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

    def mock_sports_record(self):
        with db.atomic():
            cot = SportsRecord.select().count()
            if cot == 0:

                dic_data = list(Dictionary.select())
                dic_cgy_data = list(DictionaryCategory.select())

                for i in range(1):
                    sports_record_datas = [
                        self.__mock_sports_record_item(dic_data, dic_cgy_data) for j in range(1000)]
                    # logger.info(sports_record_datas)
                    SportsRecord.insert_many(sports_record_datas).execute()
                    logger.info('insert {} item to sports_record.'.format(
                        len(sports_record_datas)))

    def __print_sports_record_item(self, item, dic_data, dic_cgy_data):

        item.site = Linq(item.site.split(',')).select(
            lambda x: get_id_by_name(dic_data, x)).to_list()

        item.equipment = Linq(item.equipment.split(',')).select(
            lambda x: get_id_by_name(dic_data, x)).to_list()

        item.item = Linq(item.item.split(',')).select(
            lambda x: get_id_by_name(dic_data, x)).to_list()

        # logger.info(model_to_dict(item))
        return model_to_dict(item)

    def print_sports_record(self, size):
        with db.execution_context():
            dic_data = list(Dictionary.select())
            dic_cgy_data = list(DictionaryCategory.select())
            record_data = SportsRecord.select().limit(size)
            # df = pd.DataFrame(list(record_data.dicts()))
            dics = [self.__print_sports_record_item(
                itr, dic_data, dic_cgy_data) for itr in record_data]
            df = pd.DataFrame(dics)
            df = split_data_frame_list(df, 'site')
            df = split_data_frame_list(df, 'equipment')
            df = split_data_frame_list(df, 'item')
            print(df.head())


srm = SportsRecordMocker()
srm.init_table()
srm.mock_dic_cgy()
srm.mock_dic()
srm.mock_person()
srm.mock_sports_record()
srm.print_sports_record(10)
if not db.is_closed():
    db.close()
