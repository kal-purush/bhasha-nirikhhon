"""
This is a Flask application that provides a basic blog functionality.

The application allows users to make posts that are stored in a MongoDB database. Each post consists of some content and
a date, which is automatically set to the current date when the post is made.

The application is served at the root URL ("/"). If a GET request is made to this URL, the application responds with
a page showing all the blog posts in the database. If a POST request is made, the application treats the body of the
request as a new blog post, stores it in the database, and then shows the updated list of blog posts.

Functions:
---------
create_app(): Creates and returns a Flask app with the above-described behavior.

Environment Variables:
---------------------
MONGODB_URI : A MongoDB connection string that specifies the database where the blog posts are stored.
"""

import datetime
import os
from flask import Flask, render_template, request
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file.
load_dotenv()


def create_app():
    """
    Creates and returns a Flask application that interacts with a MongoDB database to provide basic blog functionality.

    The app provides a form at the root URL ("/") for users to submit new blog posts. Each blog post consists of some
    content, submitted via the form, and a date, which is automatically set to the current date when the post is made.

    If a GET request is made to the root URL, the application responds with a page showing all the blog posts in the
    database. If a POST request is made, the application treats the body of the request as a new blog post,
    stores it in the database, and then shows the updated list of blog posts.

    Returns:
    -------
    app : Flask
        A Flask application instance with the above-described behavior.
    """
    # Initialize an instance of Flask
    app = Flask(__name__)
    # Create a MongoClient instance with the MongoDB URI specified in an environment variable.
    client = MongoClient(os.getenv("MONGODB_URI"))
    # Store a reference to a MongoDB database called 'Blog' in the Flask application context.
    app.db = client.Blog

    # Define a route for the application's root URL ("/")
    @app.route("/", methods=["GET", "POST"])
    def home():
        # If the request is a POST request (which is sent when a new entry is submitted)
        if request.method == "POST":
            # Extract the submitted entry content from the HTTP request form data.
            entry_content = request.form.get("content")
            # Get the current date and format it as a string in the format 'YYYY-MM-DD'.
            formatted_date = datetime.datetime.today().strftime("%Y-%m-%d")
            # Insert a new document into the 'entries' collection in the MongoDB database, with the content and date of the new entry.
            app.db.entries.insert_one({"content": entry_content, "date": formatted_date})

        # Retrieve all entries from the database and format their dates as strings in the format 'MMM DD'.
        entries_with_dates = [
            (entry["content"], entry["date"], datetime.datetime.strptime(entry["date"], "%Y-%m-%d").strftime("%b %d"))
            for entry in app.db.entries.find({})
        ]
        # Render the 'home.html' template, passing the list of entries to it.
        return render_template("home.html", entries=entries_with_dates)

    return app