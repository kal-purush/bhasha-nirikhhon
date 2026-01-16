from flask_restful import Resource, reqparse, fields
from flask import session
from api.users import get_user
from utils import login_required

key = 'login'

fields = {
    'username': fields.String,
    'password': fields.String,
}


class LoginAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('username', type=str, required=True,
                                   help='No {} username provided'.format(key))
        self.reqparse.add_argument('password', type=str, required=True,
                                   help='No {} password provided'.format(key))
        super().__init__()

    def get(self):
        return self.post()

    def post(self):
        args = self.reqparse.parse_args()
        session['user'] = get_user(args['username'], args['password'])
        return session['user']

class LogoutAPI(Resource):
    decorators = [login_required]

    def get(self):
        return self.post()

    def post(self):
        session['user'] = None
        return True