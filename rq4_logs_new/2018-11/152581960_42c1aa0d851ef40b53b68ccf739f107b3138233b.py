# src models/AmenitiesModel.py

from marshmallow import fields, Schema
from . import db

class AmenityModel(db.Model):
    """
    AmenityModel
    """

    #table name
    __tablename__='amenities'

    id=db.Column(db.Integer,primary_key=True)
    description=db.Column(db.Text,nullable=True)
    type=db.Column(db.String(255),nullable=True)

    # class constructor
    def __init__(self, data):
        """
        Class constructor
        """
        self.name = data.get('description')
        self.name = data.get('type')

    def save(self):
        db.session.add(self)
        db.session.commit()


    def update(self, data):
        for key, item in data.items():
            setattr(self, key, item)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    @staticmethod
    def get_all_amenities():
        return AmenitiesModel.query.all()

    @staticmethod
    def get_one_amenity(id):
        return AmenitiesModel.query.get(id)

class AmenitySchema(Schema):
    """
    Amenity Schema
    """
    id = fields.Int(dump_only=True)
    description=fields.Str(required=True)
    type=fields.Str(required=True)