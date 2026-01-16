from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, sessionmaker

Base = declarative_base()
# replace with config setting for database
database_engine = create_engine("mysql://...")
Session = sessionmaker(bind=database_engine)


class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    capacity = Column(Integer, nullable=True)
    address = Column(String(255), nullable=False)
    city = Column(String(30), nullable=False)
    state = Column(String(15), nullable=False)
    zip = Column(String(5), nullable=False)
    about = Column(String(255), nullable=False)
    start_at = Column(String(255), nullable=False)
    posted_at = Column(DateTime(), nullable=False)
    duration = Column(Integer, nullable=False)
    interests = Column(String(255), nullable=False) 
    skills = Column(String(255), nullable=False)
    org = Column() #object or id?

    # all these fields are strings
    def __init__(self, id, name, address, city, state,
                 zip, about, start_at, posted_at, duration, interests, skills, org):
        self.id = id
        self.name = name
        self.address = address
        self.city = city
        self.state = state
        self.zip = zip
        self.about = about
        self.start_at = start_at
        self.posted_at = posted_at
        self.duration = duration
        self.interests = interests
        self.skills = skills
        self.org = org

        # Update a user (must exist)
    def updateEvent(self, event_id, update_data):
        session = Session()
        try:
            session.query(events).filter_by(id=event_id).update(json.loads(update_data))
        except:
            session.rollback()
            raise #exception of some sort
        finally:
            session.close()
