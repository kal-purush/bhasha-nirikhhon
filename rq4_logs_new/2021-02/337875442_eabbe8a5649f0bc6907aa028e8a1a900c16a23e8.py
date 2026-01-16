"""
Main module with Flask application creation
"""
from flask import Flask, render_template
from sentiment_comparison.blueprints.sentiment import sentiment

app = Flask(__name__)
app.register_blueprint(sentiment)


@app.route('/')
def index():
    """ Index route for the Flask application """
    return render_template('index.html')


if __name__ == "__main__":
    app.run(host='127.0.0.1', port='8080', debug=True)