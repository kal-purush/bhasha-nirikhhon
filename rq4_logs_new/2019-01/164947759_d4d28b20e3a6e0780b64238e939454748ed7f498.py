"""
Creat database for HP Norton to store customer information,
custommer credit information,
monthly active customer informaiont
"""

import logging
from peewee import *

log_format = "%(asctime)s %(filename)s:%(lineno)-3d %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)

database = SqliteDatabase('management_database_HP_Norton.db')
database.connect()
database.execute_sql('PRAGMA foreign_keys = ON;')

class BaseModel(Model):
    class Meta:
        database = database

class Customer(BaseModel):
    logging.info("Customer class, coresponses to customer table in database")
    """
    This class defines customer, which includes information of Customer ID.
    Name. Lastname. Home address. Phone number. Email address. Credit limit.
    """

    customer_id = CharField(primary_key = True, unique = True, max_length=30)
    customer_name = CharField(max_length=30)
    last_name = CharField(max_length=30)
    home_address = CharField(max_length=50)
    phone_number = IntegerField()
    email = TextField()
    credit_limit = DecimalField(decimal_places=2)

class Sale(Model):
    logging.info("Sale class, keep track of monthly sale")
    """
    keep track of montly sales, if a customer has sales in this month,
    this customer would be seen as active customer.
    """