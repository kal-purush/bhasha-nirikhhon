from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from app.models import db, Note

note_routes = Blueprint('notes', __name__)


# CREATE A NEW note
@note_routes.post('')
@login_required
def post_new_note():
    pass


# READ SINGLE note
@note_routes.get('/<int:id>')
@login_required
def get_note(id):
    pass


# UPDATE note
@note_routes.put('/<int:id>')
@login_required
def edit_note(id):
    pass


# DELETE note
@note_routes.delete('/<int:id>')
@login_required
def delete_note(id):
    pass