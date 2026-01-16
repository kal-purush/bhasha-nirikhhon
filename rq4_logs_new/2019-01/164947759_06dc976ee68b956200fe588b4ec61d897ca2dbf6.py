"""
    Simple database examle with Peewee ORM, sqlite and Python
    Here we define the schema
    Use logging for messages so they can be turned off

"""

from peewee import *

DATABASE = SqliteDatabase('customer.db')
DATABASE.connect()
DATABASE.execute_sql('PRAGMA foreign_keys = ON;')

# class BaseModel(Model):
#     class Meta:
#         database = database


class Customer(Model):
    """
        This class defines Person, which maintains details of someone
        for whom we want to research career to date.
    """
    customer_id = IntegerField(primary_key=True)
    first_name = CharField(max_length=40)
    last_name = CharField(max_length=40)
    home_address = TextField()
    phone_number = FixedCharField(max_length=12)
    email_address = CharField(max_length=40)
    status = BooleanField()
    credit_limit = DoubleField()

    class Meta:
        '''
        Meta class
        '''
        database = DATABASE