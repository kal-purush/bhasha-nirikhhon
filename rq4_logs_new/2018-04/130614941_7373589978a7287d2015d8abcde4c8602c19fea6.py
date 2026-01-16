from app import db


class Book(object):
    pass


class User(db.DynamicDocument):
    email = db.StringField(required=True, unique=True)
    # hash_pw = db.BinaryField(required=True)
    password = db.BinaryField()
    firstname = db.StringField(default='')
    lastname = db.StringField(default='')

    meta = {'collection': 'users'}