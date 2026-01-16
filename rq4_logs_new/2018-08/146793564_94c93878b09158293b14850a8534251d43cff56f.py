import ast
import datetime
import json
import os
import re

from flask import Flask, flash, jsonify, redirect, render_template, request, session
from flask_session import Session
from cs50 import SQL, eprint
from helpers import lookup, lookup_details
from tempfile import mkdtemp
from werkzeug.exceptions import default_exceptions
from werkzeug.security import check_password_hash, generate_password_hash
from passlib.context import CryptContext
from pprint import pprint
from yelpapi import YelpAPI

# Configure application
app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False

# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_FILE_DIR"] = mkdtemp()
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

# Configure CS50 Library to use SQLite database
db = SQL("sqlite:///decide.db")

# Configure Yelp API key. https://www.yelp.com/developers/v3/manage_app
api_key = "JPSD1KTG7J2HiXxo4r63wcWAqxMmj0lElhIOl-43xT4HYcEM4WiCbStLOG96HDtIeQMF6eSZF0AXntwxRTEhsCuSF2pY2zNF_rZmVF7JfB3iBe6EDVLf0oW30M1xW3Yx"
yelp_api = YelpAPI(api_key)



# Ensure responses aren't cached
@app.after_request
def after_request(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/")
def index():
    """Render map"""
    return render_template("index.html")


@app.route("/articles", methods=["GET", "POST"])
def articles():
    """Look up articles for geo"""

    # Ensure parameters are present
    geo = request.args.get("geo")
    if not geo:
        raise RuntimeError("missing geo")

    # Look up articles for geo
    articles = lookup(geo)
    return jsonify(articles)


@app.route("/search", methods=["GET", "POST"])
def search():
    """Search for places that match query"""

    # Ensure parameters are present
    q = request.args.get("q") + "*"
    if not q:
        raise RuntimeError("missing q")

    # Look up places for q
    places = db.execute("SELECT * FROM v_places WHERE v_places MATCH :q", q=q)

    return jsonify(places)


@app.route("/location", methods=["POST"])
def location():
    """Get current location of user and store businesses for that location"""

    location = request.get_json();
    location = {"latitude": location["latitude"], "longitude": location["longitude"]}

    # convert location dictionary to string
    str_location = str(location)

    # query database for location
    location_businesses = db.execute("SELECT * FROM location_businesses WHERE location = :location", location=str_location)

    # if location is in database, no need to look up businesses in this location on yelp
    if len(location_businesses) == 1:

        # location must already be in database
        location_businesses = location_businesses[0]["businesses"]
        location_businesses = ast.literal_eval(location_businesses)

        # if business details are in database, no need to look up on yelp
        business_details = db.execute("SELECT * FROM businesses WHERE location = :location", location=str_location)
        if len(business_details) >= 1:
            eprint("we have details for location")

            # The server successfully processed the request and is not returning any content
            return ("", 204)

        else:

            # lookup location details and return results to the front end
            business_details = lookup_details(str_location, location_businesses)

            # The server successfully processed the request and is not returning any content
            return ("", 204)

    # look up businesses for this location
    else:

        # call yelp api for businesses in this location
        response = yelp_api.search_query(categories='restaurants', latitude=location["latitude"], longitude=location["longitude"], radius=16000, sort_by='rating')
        location_businesses = response["businesses"]
        str_location_businesses = str(location_businesses)

        # add businesses for this location to the database
        db.execute("INSERT INTO location_businesses (location, businesses) VALUES (:location, :businesses)", location=str_location, businesses=str_location_businesses)

        # query database for location
        location_businesses = db.execute("SELECT * FROM location_businesses WHERE location = :location", location=str_location)

        # if location is in database, no need to look up businesses in this location on yelp
        if len(location_businesses) == 1:

            # location must already be in database
            location_businesses = location_businesses[0]["businesses"]
            location_businesses = ast.literal_eval(location_businesses)

            # if business details are in database, no need to look up on yelp
            business_details = db.execute("SELECT * FROM businesses WHERE location = :location", location=str_location)
            if len(business_details) >= 1:
                eprint("we have details for location")

                # The server successfully processed the request and is not returning any content
                return ("", 204)

            else:

                # lookup location details and return results to the front end
                business_details = lookup_details(str_location, location_businesses)

                # The server successfully processed the request and is not returning any content
                return ("", 204)



@app.route("/businesses", methods=["GET"])
def businesses():
    """Get business details for location"""

    # Ensure parameters are present
    location = request.args.get("location")
    if not location:
        raise RuntimeError("missing location parameter")

    # location = str(location)
    location = json.loads(location)
    location = str(location)
    eprint(location)

    # Look up places for q
    # if business details are in database, no need to look up on yelp
    business_details = db.execute("SELECT * FROM businesses WHERE location = :location", location=location)
    if len(business_details) >= 1:
        eprint("we have details for location")

        # success
        return jsonify(business_details)

    else:

        # we need location businesses
        raise RuntimeError("missing location businesses")



@app.route("/update")
def update():
    """Find up to 10 places within view"""

    # Ensure parameters are present
    if not request.args.get("sw"):
        raise RuntimeError("missing sw")
    if not request.args.get("ne"):
        raise RuntimeError("missing ne")

    # Ensure parameters are in lat,lng format
    if not re.search("^-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?$", request.args.get("sw")):
        raise RuntimeError("invalid sw")
    if not re.search("^-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?$", request.args.get("ne")):
        raise RuntimeError("invalid ne")

    # Explode southwest corner into two variables
    sw_lat, sw_lng = map(float, request.args.get("sw").split(","))

    # Explode northeast corner into two variables
    ne_lat, ne_lng = map(float, request.args.get("ne").split(","))

    # Find 10 cities within view, pseudorandomly chosen if more within view
    if sw_lng <= ne_lng:

        # Doesn't cross the antimeridian
        rows = db.execute("""SELECT * FROM places
                          WHERE :sw_lat <= latitude AND latitude <= :ne_lat AND (:sw_lng <= longitude AND longitude <= :ne_lng)
                          GROUP BY country_code, place_name, admin_code1
                          ORDER BY RANDOM()
                          LIMIT 10""",
                          sw_lat=sw_lat, ne_lat=ne_lat, sw_lng=sw_lng, ne_lng=ne_lng)

    else:

        # Crosses the antimeridian
        rows = db.execute("""SELECT * FROM places
                          WHERE :sw_lat <= latitude AND latitude <= :ne_lat AND (:sw_lng <= longitude OR longitude <= :ne_lng)
                          GROUP BY country_code, place_name, admin_code1
                          ORDER BY RANDOM()
                          LIMIT 10""",
                          sw_lat=sw_lat, ne_lat=ne_lat, sw_lng=sw_lng, ne_lng=ne_lng)

    # Output places as JSON
    return jsonify(rows)