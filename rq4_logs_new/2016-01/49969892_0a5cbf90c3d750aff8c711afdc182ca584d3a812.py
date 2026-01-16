import sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()

class F5Device(Base):
    __tablename__ = 'f5_device'

    id = Column(Integer, primary_key=True)
    ipAddress = Column(String(20), nullable=False)
    hostName = Column(String(50))
    details = Column(String(250))
    apiUserName = Column(String(50))
    apiPassword = Column(String(50))

class AFMStat(Base):
    __tablename__ = 'afm_stat'

    id = Column(Integer, primary_key=True)
    f5Device_Id = Column(Integer, ForeignKey('f5_device.id'))
    statType = Column(String(20))
    statDescription = Column(String(250))
    statURL = Column(String(250), nullable=False)

class StatValue(Base):
    __tablename__ = 'stat_value'

    id = Column(Integer, primary_key=True)
    afmStat_id = Column(Integer, ForeignKey('afm_stat.id'))
    dateTime = Column(DateTime)
    statValue = Column(Integer)


engine = create_engine('sqlite:///F5AFM_App.db')

Base.metadata.create_all(engine)