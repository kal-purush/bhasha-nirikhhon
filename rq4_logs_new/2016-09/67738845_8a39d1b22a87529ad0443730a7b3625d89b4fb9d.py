#!/usr/bin/env python3

from datetime import datetime

from flask import Flask, jsonify, request, abort
from flask_sqlalchemy import SQLAlchemy

from .validators import json_params_exist

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///../database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class Message(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=False)
    message = db.Column(db.Text(), unique=False)
    created_date = db.Column(db.DateTime(), unique=False)

    def __init__(self, username, message):
        self.username = username
        self.message = message
        self.created_date = datetime.utcnow()

    def serialize(self):
        return {
            'id': self.id,
            'username': self.username,
            'message': self.message,
            'created_date': self.created_date,
        }

    def __repr__(self):
        return '<Message {0} {1}>'.format(self.username, self.message)


@app.route("/api/v1.0/create_message/", methods=['POST'])
def crete_message():

    valid, message = json_params_exist(request, ['username', 'message'])
    if not valid:
        abort(400, {'message': message})

    m = Message(request.json['username'], request.json['message'])
    db.session.add(m)
    db.session.commit()

    return jsonify({"created": True, 'id': m.id})


@app.route("/api/v1.0/delete_message/<int:id>/", methods=['DELETE'])
def delete_message(id):
    m = Message.query.filter_by(id=id).first()
    if m:
        db.session.delete(m)
        db.session.commit()
        return jsonify({"deleted": True})

    abort(404, "Message with id {} doesn't exist.".format(id))


@app.route("/api/v1.0/get_messages/")
def get_messages():
    messages = Message.query.all()
    return jsonify(json_list=[m.serialize() for m in messages])