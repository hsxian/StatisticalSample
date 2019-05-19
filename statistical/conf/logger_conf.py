import logging
import os
# from logging.handlers import HTTPHandler
import sys
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# StreamHandler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# FileHandler
log_dir='logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

file_handler = logging.FileHandler('{}/{:%Y-%m-%d}.log'.format(log_dir,datetime.datetime.now()))
file_handler.setLevel(level=logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# HTTPHandler
# http_handler = HTTPHandler(host='localhost:8001', url='log', method='POST')
# logger.addHandler(http_handler)
