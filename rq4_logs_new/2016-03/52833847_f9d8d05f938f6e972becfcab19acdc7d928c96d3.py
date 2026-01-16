from user import User
from sqlalchemy import *


class Admin(User):
    __tablename__ = 'admins'
    id = Column(Integer, ForeignKey('users.id'), primary_key=True, nullable=False)
    # the only additional field an admin has is the master admin flag
    master = Column(Boolean, nullable=false)

    __mapper_args__ = {'polymorphic_identity' : 'admin'}


    def __init__(self, id, name, email, passwordhash, phone, last_activity, master, birthdate=None,
             about=None, gender=None):
        User.__init__(self, id, name, email, passwordhash, phone, 'admin', last_activity)
        self.master = master

