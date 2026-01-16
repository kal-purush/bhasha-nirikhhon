from flask.views import MethodView
from flask import send_file

class ServerController(MethodView):
    def get(self):
        return send_file('util/static/img/favicon.ico', as_attachment=True)