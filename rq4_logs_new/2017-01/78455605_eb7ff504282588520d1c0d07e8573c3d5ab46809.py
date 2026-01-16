from flask import Blueprint, jsonify
from flask import request

from app import db
from app.mod_note.model import Note
from flask_jwt import jwt_required, current_identity

mod_note = Blueprint('note', __name__, url_prefix='/api')


@mod_note.route('/note', methods=['GET'])
@jwt_required()
def get_notes():
    notes = Note.query.filter_by(user_id=current_identity.id).all()
    return jsonify(notes=[n.serialize() for n in notes]), 200


@mod_note.route('/note', methods=['POST'])
@jwt_required()
def post_note():
    data = request.get_json(True)
    note = Note(data['title'], data['content'], data['category_id'], current_identity.id)
    db.session.add(note)
    db.session.commit()
    return jsonify(message="Note created"), 201


@mod_note.route('/note/<int:note_id>')
@jwt_required()
def get_note(note_id):
    note = Note.query.filter_by(id=note_id).first()
    return jsonify(note.serialize()), 200