from werkzeug.security import safe_str_cmp

# local import
from user import Users

users = [
    Users(1, 'admin', 'admin')    
]

"""Find a user using the username"""
username_mapping = {us.username: us for us in users}

"""Find a user using the id"""

userid_mapping = {us.id: us for us in users}


"""Authenticate a user"""
def auth(username, password):
    user = username_mapping.get(username, None)
    if user and user.password == password:
        return user

"""using the payload"""
def identity(payload):
    user_id = payload['identity']
    return userid_mapping.get(user_id, None)