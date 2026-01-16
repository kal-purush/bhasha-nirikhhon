from .db import db, environment, SCHEMA, add_prefix_for_prod
from datetime import datetime

class Note(db.Model):
    __tablename__ = "notes"

    if environment == "production":
        __table_args__ = { 'schema': SCHEMA }

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey(add_prefix_for_prod('users.id')), nullable=False)
    notebook_id = db.Column(db.Integer, db.ForeignKey(add_prefix_for_prod('notebooks.id')), nullable=False)
    title = db.Column(db.String(150))
    content = db.Column(db.String)
    trash = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    notebook = db.relationship("Notebook", back_populates="notes")

    def to_dict(self):
        return {
            'id': self.id,
            'notebookId': self.notebook_id,
            'title': self.title,
            'content': self.content,
            'createdAt': self.created_at,
            'updatedAt': self.updated_at
        }