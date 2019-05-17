from cacheout import Cache
import time
cache = Cache(maxsize=256, ttl=0, timer=time.time, default=None)
