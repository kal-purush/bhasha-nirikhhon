# src/models/UserModel.py

from marshmallow import fields, Schema
from . import db

class ReviewModel(db.Model):
    """
    ReviewModel
    """

    #table name
    __tablename__='reviews'

    id=db.Column(db.Integer,primary_key=True)
    review_value=db.Column(db.Integer,nullable=False)
    content=db.Column(db.Text,nullable=True)
    user_id=db.Column(db.Integer,db.ForeignKey('users.id'),nullable=False)

    # class constructor
    def __init__(self, data):
        """
        Class constructor
        """
        self.review_value = data.get('review_value')
        self.content = data.get('content')
        self.user_id=data.get('owner_id')


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
    def get_all_reviews():
        return ReviewModel.query.all()

    @staticmethod
    def get_one_review(id):
        return ReviewModel.query.get(id)

    class BlogpostSchema(Schema):
        """
        Review Schema
        """
        id = fields.Int(dump_only=True)
        review_value=fields.Str(required=True)
        content = fields.Str(required=True)
        user_id = fields.Int(required=True)