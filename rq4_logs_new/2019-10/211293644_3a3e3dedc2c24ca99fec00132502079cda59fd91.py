from flask import Flask
from flask import render_template
from flask import request
from flask import url_for
import json

app = Flask(__name__)
app.secret_key = 'very secret string'

pizzaer = []

@app.route("/")
@app.route('/index')
def index():
    return render_template('index.html', pizzaer = pizzaer)


def addPizza(name, price, description):
    pizzaer.append({'name' : name, 'price' : price, 'description' : description})
    savePizzaer()

def savePizzaer():
    with open("pizza_file.json", "w") as write_file:
        json.dump(pizzaer, write_file)

if __name__ == "__main__":
    with app.app_context():
        with open('static/pizza_file.json', "r") as read_file:
            pizzaer = json.load(read_file)

    app.run(debug=True)