from flask import session

from app import db


class User(db.Model):
    id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    email = db.Column(db.String(64), unique=True, nullable=False)
    password = db.Column(db.String(256), nullable=False)
    name = db.Column(db.String(32), nullable=False)

    def __init__(self, email, password, name):
        self.email = email
        self.password = password
        self.name = name

    def __repr__(self):
        return f'<User> name: {self.name}, email: {self.email}'

    @classmethod
    def get_by_session(cls):
        email = session.get('email', None)
        if email is not None:
            return cls.query.filter_by(email=email).first()
        else:
            return None

    @staticmethod
    def logout():
        session.pop('email', None)