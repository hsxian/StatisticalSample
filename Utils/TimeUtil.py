import datetime
import random


def randomtimes(start, end, n):
    return [random.random() * (end - start) + start for _ in range(n)]


def randomtime(start, end):
    return random.random() * (end - start) + start


def random_start_end_time(start, end, max_timedelta=None):
    t1 = randomtime(start, end)
    if max_timedelta:
        t2 = randomtime(t1, t1+max_timedelta)
    else:
        t2 = randomtime(t1, end)
    return [t1, t2]
