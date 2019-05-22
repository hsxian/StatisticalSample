from statistical.conf.cache_conf import cache
from statistical.conf.database_conf import db
from statistical.db.models.sports import DictionaryCategory, Dictionary, SportsRecord
from statistical.db.utils import pw_lst_2_py_dic


class SportsDao():

    @property
    def sports_dict(self):
        dic = cache.get('sports_dict')
        if not dic:
            print('read sport dict data.')
            with db.execution_context():
                data = list(Dictionary.select(Dictionary.id, Dictionary.name))
                dic = pw_lst_2_py_dic(data)
                cache.set('sports_dict', dic, ttl=60 * 10)
        return dic

    @property
    def sports_cgy_dict(self):
        dic = cache.get('sports_cgy_dict')
        if not dic:
            print('read sport category dict data.')
            with db.execution_context():
                data = list(DictionaryCategory.select(DictionaryCategory.id, DictionaryCategory.name))
                dic = pw_lst_2_py_dic(data)
                cache.set('sports_cgy_dict', dic, ttl=60 * 10)
        return dic

    def __get_selections(self, selection_names):
        querys = []
        if isinstance(selection_names, str):
            query = self.__get_class_pro(selection_names)
            if query:
                querys.append(query)
        elif isinstance(selection_names, list):
            for name in selection_names:
                query = self.__get_class_pro(name)
                if query:
                    querys.append(query)
        return querys

    def __get_class_pro(self, name):
        if hasattr(SportsRecord, name):
            return getattr(SportsRecord, name)

    def get_sports_records(self,selections,start_time,end_time):

        if isinstance(selections[0],str):
            selections=self.__get_selections(selections)

        sports_records = SportsRecord.select(*selections) \
            .where(SportsRecord.start_time < end_time) \
            .where(SportsRecord.end_time > start_time)
        return sports_records
# print(SportsDao().sports_cgy_dict['1'])
# print(SportsDao().sports_dict['1'])
