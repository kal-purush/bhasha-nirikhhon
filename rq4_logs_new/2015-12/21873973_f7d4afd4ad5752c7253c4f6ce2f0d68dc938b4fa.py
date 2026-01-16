from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, Time

import settings

DeclarativeBase = declarative_base()

def db_connect():
    return create_engine(URL(**settings.DATABASE))
	
def create_fares_table(engine):
    DeclarativeBase.metadata.create_all(engine)
	
class Fares(DeclarativeBase):
	__tablename__ = "fares"
	id = Column(Integer, primary_key=True)
	stdfare = Column('stdfare', String, nullable=True)
	advfare = Column('advfare', String, nullable=True)
	webfare = Column('webfare', String, nullable=True)
	reffare = Column('reffare', String, nullable=True)
	transfers = Column('transfers', Integer, nullable=True)
	origtime = Column('origtime', Time, nullable=True)
	desttime = Column('desttime', Time, nullable=True)
	orig = Column('orig', String, nullable=True)
	dest = Column('dest', String, nullable=True)
	duration = Column('duration', Time, nullable=True)
	timescraped = Column('timescraped', Time, nullable=True)
	datescraped = Column('datescraped', Date, nullable=True)
	date = Column('date', Date, nullable=True)
	express = Column('express', String, nullable=True)