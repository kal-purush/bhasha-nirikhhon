"""
Database models for HP Norton.
"""

import peewee
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info('Here we define our data (the schema)')
logger.info('First name and connect to a database (sqlite here)')

logger.info('The next 3 lines of code are the only database specific code')

database = SqliteDatabase('hpnorton.db')
database.connect()
database.execute_sql('PRAGMA foreign_keys = ON;') # needed for sqlite only


class BaseModel(Model):
    """ Base model class to establish database connection """

    class Meta:
        database = database



class Customer(BaseModel):
    """
        This class defines Customer. Contains all customer information
        for HP Norton.

        customerid, name, lastname, homeaddress, 
        phonenumber, email, status, creditlimit
    """

    logger.info('Note how we defined the class')

    logger.info('Specify the fields in our model, their lengths and if mandatory')
    logger.info('Must be a unique identifier for each person')
    person_name = CharField(primary_key = True, max_length = 30)
    lives_in_town = CharField(max_length = 40)
    nickname = CharField(max_length = 20, null = True)