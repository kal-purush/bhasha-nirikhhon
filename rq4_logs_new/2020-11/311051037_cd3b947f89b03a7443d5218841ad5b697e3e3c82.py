from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper


def create_session():
    engine = create_engine('sqlite:///db.sqlite', echo=False)
    Session = sessionmaker(bind=engine)

    mapper(User, users_table)
    return Session(), engine


class User(object):
    def __init__(self, user, password):
        self.user = user
        self.password = password

engine = create_engine('sqlite:///db.sqlite', echo=False)


metadata = MetaData()
users_table = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('user', String, nullable=False, unique=True,
           sqlite_on_conflict_unique='FAIL'),
    Column('password', String)
)

metadata.create_all(engine)