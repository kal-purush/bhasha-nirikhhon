# src/models/UserModel.py

from marshmallow import fields, Schema
from . import db

class UserModel(db.Model):
    """
    UserModel
    """

    #table name
    __tablename__='users'

    id=db.Column(db.Integer,primary_key=True)
    name=db.Column(db.String(128), nullable=False)
    email=db.Column(db.String(128),unique=true,nullable=False)
    password=db.Column(db.String(255),nullable=False)


    # class constructor
    def __init__(self, data):
        """
        Class constructor
        """
        self.name = data.get('name')
        self.email = data.get('email')
        self.password = self.__generate_hash(data.get('password'))

    def __generate_hash(self, password):
        return bcrypt.generate_password_hash(password, rounds=10).decode("utf-8")


    def check_hash(self, password):
        return bcrypt.check_password_hash(self.password, password)


    def save(self):
        db.session.add(self)
        db.session.commit()


    def update(self, data):
        for key, item in data.items():
            if key == 'password': # add this new line
                self.password = self.__generate_hash(value)
            setattr(self, key, item)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    @staticmethod
    def get_all_users():
        return UserModel.query.all()

    @staticmethod
    def get_one_user(id):
        return UserModel.query.get(id)