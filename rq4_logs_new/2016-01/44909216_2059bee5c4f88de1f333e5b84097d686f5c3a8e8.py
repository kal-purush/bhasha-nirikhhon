from meerkat_api import app
from flask import abort, request
from functools import wraps


def require_api_key(f):
    """
    @param f: flask function
    @return: decorator, return the wrapped function or abort json object.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.args.get('api_key') == app.config["API_KEY"] or app.config["API_KEY"] == "":
            return f(*args, **kwargs)
        else:
            app.logger.warning("Unauthorized address trying to use API: {}".format(request.remote_addr))
            abort(401)
    return decorated