import datetime

from statistical.utils.time_util import split_start_end_time_to_list

s = datetime.datetime.now()
e = datetime.datetime.now() + datetime.timedelta(days=5)

print(s, e)
print(split_start_end_time_to_list(s, e, '2Y'))
print(split_start_end_time_to_list(s, e, 'M'))
print(split_start_end_time_to_list(s, e, '2D'))
print(split_start_end_time_to_list(s, e, 'W'))
print(split_start_end_time_to_list(s, e, '19H'))
