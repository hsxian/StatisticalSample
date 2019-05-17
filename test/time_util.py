import datetime
import sys
import os

sys.path.append(os.getcwd())  # 将整个项目加入解析器的搜索目录

from statistical.utils.time_util import split_start_end_time_to_list

s = datetime.datetime.now()
e = datetime.datetime.now() + datetime.timedelta(days=2)

print(s, e)
print(split_start_end_time_to_list(s, e, 'Y'))
print(split_start_end_time_to_list(s, e, 'M'))
print(split_start_end_time_to_list(s, e, 'D'))
print(split_start_end_time_to_list(s, e, 'W'))
print(split_start_end_time_to_list(s, e, 'H'))