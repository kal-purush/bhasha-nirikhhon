from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('sqlite:///database.sqlite')
db_session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    email = Column(String(120), unique=True)
    password_hash = Column(String(120))
    last_name = Column(String(50))

    def __init__(self, first_name, email, password_hash,
                 last_name=None):
        self.first_name = first_name
        self.email = email
        self.password_hash = password_hash
        self.last_name = last_name

    def __repr__(self):
        return '<User {} {} {}>'.format(self.first_name, self.last_name, self.email)


if __name__ == "__main__":
    Base.metadata.create_all(bind=engine)

