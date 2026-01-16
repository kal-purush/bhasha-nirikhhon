from flask import Blueprint, Response, request
from db.get_manga_details import get_manga_details
from db.search_for_manga import search_for_manga

# ! == About ==
# This is our API endpoints for the web server, this connects and hands request made from client to use items in the database

api = Blueprint("api", __name__, url_prefix="/API")

# ! == Query setup ==
# * Example url /API/manga/69
# - book_id is the id of the manga in the database
@api.route("/manga/<int:book_id>", methods=["GET"])
def manga_info(book_id: int):

    #! impliment get_manga_info

    # Placeholder code can remove
    return Response("bookid = " + str(book_id), status=200, mimetype="text/plain")


# ! == Query setup ==
# NOTE: limit number of manga return to be 25 to 50
# * Example url /API/search?query=helloworld&type=author&lastbookid=10
# - type = title | author | theme | genre, defualt is title
# - lastbookid = some int, default is 1
# - query = user search term, this is required to run
@api.route("/search", methods=["GET"])
def search():

    #! impliment search_for_manga

    # Placeholder code can remove
    return Response("args = " + str(request.args), status=200, mimetype="text/plain")