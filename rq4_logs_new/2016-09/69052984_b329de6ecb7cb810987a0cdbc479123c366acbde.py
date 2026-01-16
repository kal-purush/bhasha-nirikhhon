from flask import Blueprint, redirect, jsonify
from flask_restful import Api

from .resources.series import SeriesDetail, SeriesList
from RanobeHonyaku.utils import setup_file

# Set up Flask-RESTful and add resources
api_v1 = Blueprint("api", __name__, url_prefix="/api/v1")
api = Api(api_v1)
api.add_resource(SeriesDetail, "/series/<int:series_id>")
api.add_resource(SeriesList, "/series")


# This is the base for the API; Will redirect to our Github organisation page so it always links to latest documentation
@api_v1.route("/")
def root():
    return redirect(setup_file["GITHUB"], 301)


# Our class for API errors to raise so we can catch it with our error_handler function
# Note that this currently isn't being used - Flask-RESTful can catch its own errors, but it should
# be possible to manually throw errors if that was what you were going for
class APIError(Exception):

    def __init__(self, message: str, status_code: int):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code

    def format_to_dict(self):
        return {"message": self.message,
                "error": self.status_code}


# Handles all errors that involve the API; Will return an error message and the response code
@api_v1.errorhandler(APIError)
def api_error_handler(error):
    response = jsonify(error.format_to_dict())
    return response, error.status_code