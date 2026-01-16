from functools import wraps
from flask import make_response
import json


def accept_json(request):
    """This decorator checks if the format of content comes with a request object is JSON or not. \n
    If it is not a response 'Unsupported Media-Type' is returned

    Keyword arguments:
    request -- the Http request object
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                json.loads(request.data.decode())
                if request.json:
                    return func(*args, **kwargs)
            except Exception:
                resp = make_response(json.dumps({'error': 'Invalid JSON'}), 422)
                resp.mimetype = 'application/json'
                return resp
            resp = make_response(json.dumps({'error': 'Unsupported media type'}), 415)
            resp.mimetype = 'application/json'
            return resp
        return wrapper
    return decorator