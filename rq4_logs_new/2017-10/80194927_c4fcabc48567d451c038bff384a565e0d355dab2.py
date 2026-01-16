from flask import Flask, render_template, request, jsonify
from flask_cors import CORS

app = Flask(__name__, static_folder="dist/static", template_folder='dist')
CORS(app)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/json")
def api():
    return jsonify(title="API Test successful!", greeting="Salut!")


app.secret_key = "secret123"
app.jinja_env.auto_reload = True
app.config['TEMPLATES_AUTO_RELOAD'] = True