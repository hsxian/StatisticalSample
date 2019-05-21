from peewee import PostgresqlDatabase, MySQLDatabase, SqliteDatabase


def postgres():
    return PostgresqlDatabase(
        'testdb',
        user='postgres',
        password='123',
        host='127.0.0.1',
    )


def mysql():
    return MySQLDatabase(
        'testdb',
        user='root',
        password='123456',
        host='127.0.0.1',
    )


def sqlite():
    return SqliteDatabase('testdb.db')


db = postgres()
