import datetime
import random


def randomtimes(start, end, n):
    return [random.random() * (end - start) + start for _ in range(n)]


def random_start_end_time(start, end):
    while True:
        result = randomtimes(start, end, 2)
        if (result[0] < result[1]):
            return result
