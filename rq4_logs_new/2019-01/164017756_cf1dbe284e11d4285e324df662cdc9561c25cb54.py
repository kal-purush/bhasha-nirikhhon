import re
from flask import abort, make_response, jsonify, request
from functools import wraps
import uuid
import jwt
import os

from app.api.v1.models.users import userslist

db = userslist

class InputValidation():
    """Validators Class"""
  
    def check_state(self,data):
        for k,v in data.items():
            if k == "" or v== "":
                make_response(jsonify({"status": 400, "error": "empty fields"}), 400)

    def valid_name(self, name):
        """validate username"""
        regex = "^[a-zA-Z]{4,}$"
        return re.match(regex, name)

    def check_username(self, username):
        """Method for checking if username exist"""
        user = [user for user in db if user['username'] == username]
        if user:
            return True
        return False
    
    def valid_email(self, email):
        """ valid email """
        regex = "^[\w]+[\d]?@[\w]+\.[\w]+$"
        return re.match(regex, email)

    def check_email(self, email):
        """Method for checking if user email exist"""
        user = [user for user in db if user['email'] == email]
        if user:
            return user
    
    def valid_password(self, password):
        """ valid password """
        regex = "^[a-zA-Z0-9@_+-.]{5,}$"
        return re.match(regex, password)

    def token_required(self, f):
        @wraps(f)
        def decorated(*args, **kwargs):
            token = None
            if 'x-access-token' in request.headers:
                token = request.headers['x-access-token']
            if not token:
                return jsonify({'message': "Token is missing"})
            try:
                data = jwt.decode(token, os.getenv("SECRET_KEY"))
                current_user = [user for user in db if user['public_id'] == data['public_id']]
            except:
                return jsonify({'message': "Token invalid"})
            return f(current_user, *args, **kwargs)
        return decorated