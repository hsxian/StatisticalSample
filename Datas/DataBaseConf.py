from peewee import *

db = PostgresqlDatabase(
    'testdb',
    user='postgres',
    password='123',
    host='127.0.0.1',
)
