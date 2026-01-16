from flask import Flask, redirect

app = Flask(__name__)

@app.route("/")
def home():
    return '<p>redirect to home page</p>'


@app.route("/home")
def home():
    return """<p>home page</p>
              <p></p>"""



@app.route("/about")
def home():
    return "<p>about page</p>"


@app.route("/appointment")
def home():
    return "<p>make an appointment page - add online scheduling</p>"

