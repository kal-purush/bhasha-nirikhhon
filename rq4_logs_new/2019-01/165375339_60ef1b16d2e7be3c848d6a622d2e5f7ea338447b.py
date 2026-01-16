"""
Module containing database models for everything concerning TransactionBeverage entries.
"""
from sqlalchemy.sql import func

from .. import DB
from . import STD_STRING_SIZE
from .beverage import Beverage
from .user import User
from .transaction import Transaction

__all__ = [ 'TransactionBeverage', ]


class TransactionBeverage(DB.Model):
    """
    The representation of a TransactionBeverage Entry
    """

    __tablename__ = 'TransactionBeverage'

    transaction_id = DB.Column(DB.Integer, DB.ForeignKey(Transaction.id), primary_key=True)
    beverage_id = DB.Column(DB.Integer, DB.ForeignKey(Beverage.id), primary_key=True)
    beverage_count = DB.Column(DB.Integer, nullable=True)
    price = DB.Column(DB.Double, nullable=True)

    transaction = DB.relationship(Transaction, lazy='joined')
    beverage = DB.relationship(Beverage, lazy='joined')

    def __init__(self):
        self.transaction = None
        self.beverage = None
        self.beverage_count = None
        self.price = 0.0