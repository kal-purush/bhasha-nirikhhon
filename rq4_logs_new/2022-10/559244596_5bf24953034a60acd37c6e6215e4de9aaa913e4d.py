from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Identity
from sqlalchemy.ext.declarative import declarative_base

DeclarBase = declarative_base()


class Users(DeclarBase):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column('name', String, nullable=False)
    surname = Column('surname', String, nullable=False)
    user_name = Column('user_name', String, Identity(), nullable=False)
    password = Column('password', String, nullable=False)
    number_phone = Column('number_phone', String, nullable=False)
    job_title = Column('job_title', String, nullable=False)
    role = Column('role', choices=(('admin', 'Администратор'), ('user', 'Пользователь')), default='user')

    def __repr__(self):
        return "".format(self.code)