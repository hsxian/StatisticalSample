import os
import sys

sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录

from statistical.conf.cache_conf import cache
from statistical.db.models.sports import DictionaryCategory, Dictionary
from statistical.conf.database_conf import db
from statistical.conf.logger_conf import logger
from statistical.db.utils import pw_lst_2_py_dic


class SportsDao():

    @property
    def sports_dict(self):
        dic = cache.get('sports_dict')
        if not dic:
            logger.info('read sport dict data.')
            with db.execution_context():
                data = list(Dictionary.select(Dictionary.id, Dictionary.name))
                dic = pw_lst_2_py_dic(data)
                cache.set('sports_dict', dic, ttl=60*10)
        return dic

    @property
    def sports_cgy_dict(self):
        dic = cache.get('sports_cgy_dict')
        if not dic:
            logger.info('read sport category dict data.')
            with db.execution_context():
                data = list(DictionaryCategory.select(DictionaryCategory.id, DictionaryCategory.name))
                dic = pw_lst_2_py_dic(data)
                cache.set('sports_cgy_dict', dic, ttl=60*10)
        return dic

# print(SportsDao().sports_cgy_dict['1'])
# print(SportsDao().sports_dict['1'])