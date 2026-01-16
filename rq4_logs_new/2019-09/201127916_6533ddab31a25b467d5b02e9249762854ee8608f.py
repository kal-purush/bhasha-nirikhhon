from flask import jsonify
from flask_restplus import Resource
from flask_restplus.namespace import RequestParser
from flask_jwt_extended import ( create_access_token,  jwt_refresh_token_required, get_jwt_identity )

#################
# Parser        #
#################
Parser = RequestParser()
Parser.add_argument("refresh_token",
                    help="refresh the token",
                    required=True,
                    location="form")

#################
# Method        #
#################


@jwt_refresh_token_required
def Put(args):
    '''
    Refresh token
    '''
    current_user = get_jwt_identity()
    access_token = create_access_token(identity = current_user)
    return {'access_token': access_token}