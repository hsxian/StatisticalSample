from datetime import datetime


class StopWatch(object):
    def __init__(self):
        self.__enable = True
        pass

    def start(self):
        self.__start = datetime.now()
        self.__enable = True
        return self.__start

    def stop(self):
        self.__stop = datetime.now()
        self.__enable = False
        return self.__stop

    @property
    def elapsed(self):
        if self.__enable:
            return datetime.now() - self.__start
        else:
            return self.__stop - self.__start
