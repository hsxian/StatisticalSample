from Utils.TimeUtil import random_start_end_time
import random
import datetime
from Datas.PostgresqlConf import db
from Models.Sports import *
import numpy as np
import sys
import os
sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录


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
                print('drop tables: {}'.format(dbtabs))
            db.create_tables(tabs)
            print('createname tables: {}'.format(tabs))

    def mock_dic_cgy(self):
        with db.atomic():
            cot = DictionaryCategory.select().count()
            if cot == 0:
                # print(self.dic_cgy_datas)
                DictionaryCategory.insert_many(self.dic_cgy_datas).execute()
                print('insert {} item to dictionarycategory.'.format(
                    len(self.dic_cgy_datas)))

    def mock_dic(self):
        with db.atomic():
            cot = Dictionary.select().count()
            if cot == 0:
                # print(self.dic_datas)
                Dictionary.insert_many(self.dic_datas).execute()
                print('insert {} item to dictionary.'.format(len(self.dic_datas)))

    def mock_person(self):
        with db.atomic():
            cot = Person.select().count()
            if cot == 0:
                # print(self.dic_datas)
                Person.insert_many(self.person_datas).execute()
                print('insert {} item to person.'.format(
                    len(self.person_datas)))

    def __get_dic_ids(self, category_name):
        dic_cat_id = DictionaryCategory.select(
            DictionaryCategory.id).where(DictionaryCategory.name == category_name).first().id
        
        dic_ids = [str(dic_id.id) for dic_id in Dictionary.select(Dictionary.id).where(
            Dictionary.category == dic_cat_id)]

        return dic_ids

    def __random_dic_ids(self, category_name):
        dic_ids = self.__get_dic_ids(category_name)        
        cot = len(dic_ids)
        dic_ids = np.random.choice(
            dic_ids, random.randint(1, cot), replace=False)

        return ','.join(dic_ids)

    def __mock_sports_record_item(self):
        se = random_start_end_time(datetime.datetime(
            2019, 1, 1), datetime.datetime(2019, 6, 1))

        return {
            'start_time': se[0],
            'end_time': se[1],
            'site': self.__random_dic_ids('site'),
            'equipment': self.__random_dic_ids('equipment'),
            'item': self.__random_dic_ids('item'),
            'person': random.randint(1, Person.select().count()),
        }

    def mock_sports_record(self):
        with db.atomic():
            cot = SportsRecord.select().count()
            if cot == 0:
                sports_record_datas = [
                    self.__mock_sports_record_item() for i in range(10000)]
                # print(sports_record_datas)
                SportsRecord.insert_many(sports_record_datas).execute()
                print('insert {} item to sports_record.'.format(
                    len(sports_record_datas)))


srm = SportsRecordMocker()
srm.init_table()
srm.mock_dic_cgy()
srm.mock_dic()
srm.mock_person()
srm.mock_sports_record()

if not db.is_closed():
    db.close()
