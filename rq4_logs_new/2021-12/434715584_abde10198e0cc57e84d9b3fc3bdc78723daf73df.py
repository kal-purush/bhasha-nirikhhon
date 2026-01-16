from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for)

from werkzeug.exceptions import abort

from app.chef import login_required # the decorator to ensure login
from app.db import get_db

bp = Blueprint("search", __name__)

@bp.route("/request", methods=("GET", "POST"))
@login_required
def request():
	db = get_db()
	