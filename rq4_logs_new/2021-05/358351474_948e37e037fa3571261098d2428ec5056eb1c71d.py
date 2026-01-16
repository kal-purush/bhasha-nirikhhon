from sqlalchemy import (create_engine,Column, Table, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Integer, SmallInteger, String, Date, DateTime, Float, Boolean, Text, LargeBinary)

from scrapy.utils.project import get_project_settings

DeclarativeBase = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(get_project_settings().get("CONNECTION_STRING"))

def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)

class SenatorDB(DeclarativeBase):
    __tablename__ = "senators"

    id = Column('id', Integer, primary_key=True)
    id_quadra = Column('id_quadra', Integer)
    id_tipo_imovel = Column('id_tipo_imovel', Integer)
    url = Column('url', String(75))
    party = Column('party', String(15))
    fu = Column('fu', String(2))
    period = Column('period', String(12))
    phones = Column('phones', String(75))
    email = Column('email', String(45))
    endereco = Column('address', String(100))

class Ceaps(DeclarativeBase):
    __tablename__ = "ceaps"

    id = Column(Integer, primary_key=True)
    name = Column('name', String(75))
    url = Column('url', String(75))
    party = Column('party', String(15))
    fu = Column('fu', String(2))
    period = Column('period', String(12))
    phones = Column('phones', String(75))
    email = Column('email', String(45))
    address = Column('address', String(100))