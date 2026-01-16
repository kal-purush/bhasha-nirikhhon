__author__ = 'Wang'

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from lockerapp.users.model import db


class PhoneCaptcha(db.Model):
    __tablename__ = 'phone_captcha'
    id = db.Column(db.Integer, primary_key=True)
    phone = db.Column(db.String(50))
    captcha = db.Column(db.INTEGER)
    start_time = db.Column(db.INTEGER)
    del_flag = db.Column(db.INTEGER)

    def __init__(self, phone = None, captcha = None, start_time = None,del_flag = 0):
        self.phone = phone
        self.captcha = captcha
        self.start_time = start_time
        self.del_flag = del_flag

    def __repr__(self):
        return '<Post %r>' % self.phone

    def save(self):
        db.session.add(self)
        db.session.commit()