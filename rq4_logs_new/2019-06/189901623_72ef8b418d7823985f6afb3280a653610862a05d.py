#!/usr/bin/python3
"""
base_model class to be inherited by all models
"""
import models
from uuid import uuid4
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime


Base = declarative_base()

class BaseModel:
    """
    BaseModel class for all other classes
    """
    id = Column(String(60), unique=True,
                primary_key=True, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow())


    def __init__(self, *args, **kwargs):
        """
        Instantiation of base model class
        set user attributes for table
        Args:
          args - name of cls obj to create
          kwargs - assign user attributes
        """

        self.id = str(uuid4())
        self.created_at = datetime.utcnow()

        if kwargs:
            for key, value in kwargs.items():
                setattr(self, key, value)

    def all(self):
        """
        return dict of all objs in db
        """
        models.storage.all()

    def save(self):
        """
        adding obj to current db session
        """
        models.storage.new(self)
        models.storage.save()

    def delete(self, cls, code):
        """
        delete obj from db
        """
        models.storage.delete(cls, str(code))

    def close(self):
        """
        close session, empty pool
        """
        models.storage.close()