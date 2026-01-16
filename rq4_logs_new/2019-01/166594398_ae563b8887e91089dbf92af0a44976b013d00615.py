from flask import Blueprint

bp = Blueprint('names', __name__)

from app.names import routes