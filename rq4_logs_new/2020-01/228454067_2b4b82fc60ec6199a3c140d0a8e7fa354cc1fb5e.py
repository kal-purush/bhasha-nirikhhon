from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    jwt_refresh_token_required,
    get_jwt_identity)
from flask_restful import Resource, reqparse
from werkzeug.security import safe_str_cmp

from logs import Logger
from models.user_model import UserModel


class UserLogin(Resource):
    @classmethod
    def post(cls):
        logger = Logger('post::userlogin::resources::flask')
        logger.debug('Loging user')
        parser = reqparse.RequestParser()
        parser.add_argument('email',
                            type=str,
                            required=True,
                            help='This field cannot be blank'
                            )
        parser.add_argument('password',
                            type=str,
                            required=True,
                            help='This field cannot be blank'
                            )

        data = parser.parse_args()
        user = UserModel.find_by_email(data['email'])

        if user and safe_str_cmp(user.password, data['password']):
            access_token = create_access_token(identity=user.id, fresh=True)
            refresh_token = create_refresh_token(user.id)
            return {
                'access_token': access_token,
                'refresh_token': refresh_token
            }, 200

        return {'message': 'Invalid credentials'}, 401


class TokenRefresh(Resource):
    @jwt_refresh_token_required
    def post(self):
        logger = Logger('post::tokenrefresh::resources::flask')
        logger.debug('Refreshing token')
        current_user = get_jwt_identity()
        new_token = create_access_token(identity=current_user, fresh=False)
        return {'access_token': new_token}, 200