import pdb
from flask_jwt_extended import (jwt_required,verify_jwt_in_request, get_jwt_identity)
from functools import wraps
from flask import jsonify, make_response, abort


def admin_only(function):
    """allows admin to access"""
    @wraps(function)
    @jwt_required()
    def wrapper(*args, **kwargs):
        """wrapper for the function"""
       
        verify_jwt_in_request()
        claims = get_jwt_identity()
        print(claims, 'asasdasd')
        if claims[1] != 1:
            abort(
                make_response(
                    jsonify({'message': 'unauthorized to perform  function'}),
                    401
                )
            )
        return function(*args, **kwargs)
    return wrapper