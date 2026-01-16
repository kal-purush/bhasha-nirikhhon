import os
import json
from flask import Flask, render_template


app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/festlogen")
def festlogen():
    return render_template("festlogen.html")


@app.route("/varaRum")
def varaRum():
    return render_template("vara-rum.html")


@app.route("/dellenbygden")
def dellenbygden():
    return render_template("dellenbygden.html")


@app.route("/kontakt")
def kontakt():
    return render_template("kontakt.html")


if __name__ == "__main__":
    app.run(
        host=os.environ.get("IP", "0.0.0.0"),
        port=int(os.environ.get("PORT", "5000")),
        debug=True
        # debug = True only for developement, needs to be changed
        # before submition of the project
    )