author = "Alban"

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float

engine = create_engine('sqlite:///:memory', echo=True)

Base = declarative_base()

class Beacon(Base):
    __tablename__='beacons'

    id = Column(Integer, primary_key=True)
    name = Column(String(10))
    pos_x = Column(Float)
    pos_y = Column(Float)

    def __repr__(self):
        return "<Beacon(name='%s', pos_x=%f, pos_y=%f)>" % (self.name, self.pos_x, self.pos_y)



class Flight(Base):
    __tablename__='flights'

    id = Column(Integer, primary_key=True)
    callsign = Column(String(10))               # Identifiant appel pour controleur
    type = Column(String(10))                   # Type d avion
    dep = Column(String(10))                    # Aeroport de depart
    arr = Column(String(10))                    # Aeroport d arrivee
    h_dep = Column(Integer)                     # Heure de depart        A VOIR AVEC HORLOGE
    h_arr = Column(Integer)                     # Heure d arrivee        A VOIR AVEC HORLOGE
    id_flp = Column(Integer)                    # Id plan de vol associe

    def __repr__(self):
        return "<Flight(callsign='%s', type=%s, dep=%s, arr=%s, h_dep=%d, h_arr=%d, id_flp=%d)>" % \
               (self.callsign, self.type, self.dep, self.arr, self.h_dep, self.h_arr, self.id_flp)

#Base.metadata.create_all(engine)

balise = Beacon(name='Test', pos_x=10.2, pos_y=-1563.869)
vol = Flight(callsign='AF4185', type='A320', dep='LFBO', arr='LFPO', h_dep=1020, h_arr=1125, id_flp=20)

print balise
print vol