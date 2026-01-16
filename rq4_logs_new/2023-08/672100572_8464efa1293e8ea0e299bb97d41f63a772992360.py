"""Ping CRM dashboard routes."""

from flask import Blueprint, abort
from flask_inertia import render_inertia
from flask_login import login_required

dashboard_routes = Blueprint("dashboard", __name__)


@dashboard_routes.route("/")
@login_required
def index():
    return render_inertia("Dashboard")


@dashboard_routes.route("/500/", methods=["GET"])
@login_required
def error500():
    abort(500)


@dashboard_routes.route("/404/", methods=["GET"])
@login_required
def error404():
    abort(404)