import flask

app = flask.Flask(__name__)
app.config.from_pyfile('_config.py')