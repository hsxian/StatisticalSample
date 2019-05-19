from peewee import Model, PrimaryKeyField, CharField, IntegerField, ForeignKeyField, TextField, DateTimeField

from statistical.conf.database_conf import db


class Person(Model):
    id = PrimaryKeyField()
    name = CharField()
    age = IntegerField()

    class Meta:
        database = db


class DictionaryCategory(Model):
    id = PrimaryKeyField()
    name = CharField()

    class Meta:
        database = db


class Dictionary(Model):
    id = PrimaryKeyField()
    category = ForeignKeyField(DictionaryCategory)
    name = CharField()
    remarks = TextField()

    class Meta:
        database = db


class SportsRecord(Model):
    id = PrimaryKeyField()
    start_time = DateTimeField()
    end_time = DateTimeField()
    site = TextField()
    equipment = TextField()
    item = TextField()
    person = ForeignKeyField(Person)

    class Meta:
        database = db


if not db.is_closed():
    db.close()
