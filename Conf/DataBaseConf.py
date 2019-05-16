from peewee import *

# db = PostgresqlDatabase(
#     'testdb',
#     user='postgres',
#     password='123',
#     host='127.0.0.1',
# )
db = MySQLDatabase(
    'testdb',
    user='root',
    password='123456',
    host='127.0.0.1',
)
