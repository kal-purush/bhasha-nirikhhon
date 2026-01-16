from peewee import *


# class PostgresqlDatabase(db[, register_unicode=True[, encoding=None[, isolation_level=None]]]):

db = PostgresqlDatabase('contacts', user='postgres', password='',
                        host='localhost', port=5432)

db.connect()


class BaseModel(Model):
    class Meta:
        database = db
        
 
class Contacts(BaseModel):
    first_name= CharField()
    last_name = CharField()
    number =  CharField()
    email = CharField()