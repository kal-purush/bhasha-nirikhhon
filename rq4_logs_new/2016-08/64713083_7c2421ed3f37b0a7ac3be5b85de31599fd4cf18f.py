from sqlalchemy import Column, Integer, Float, Date, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import csv
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:{}@localhost/restaurantmap'.format(os.environ.get('DB_PASSWORD'))
db = SQLAlchemy(app)



##Base = declarative_base()

#Create a class for the restaurant data
class Restaurant(db.Model):
##class Restaurant(Base):
#    __tablename__ = 'Restaurants'
#    __table_args__ = {'sqlite_autoincrement': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    obj_id = Column(String(20))
    name = Column(String(150))
    building = Column(String(100))
    zip_code = Column(String(14))
    longitude = Column(Float)
    latitude = Column(Float)

    def __init__(self,obj_id, name, building, zip_code, longitude, latitude):
        self.obj_id = obj_id
        self.name = name
        self.building = building
        self.zip_code = zip_code
        self.longitude = longitude
        self.latitude = latitude

    def __repr__(self):
        return "<Restaurant(name='%s', latitude='%s, longitude='%s')>" % (self.name, self.latitude, self.longitude)

db.create_all()
