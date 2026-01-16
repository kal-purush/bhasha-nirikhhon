from flask import Flask, render_template, request, jsonify
import json

app = Flask(__name__)


@app.route('/')
def home():
    return render_template("main.html")


@app.route('/welcome')
def welcome():
    return render_template('welcome.html')


@app.route('/calendar')
def calendar():
    return render_template('calendar.html')


@app.route('/data')
def return_data():
    start_date = request.args.get('start', '')
    end_date = request.args.get('end', '')
    # You'd normally use the variables above to limit the data returned
    # you don't want to return ALL events like in this code
    # but since no db or any real storage is implemented I'm just
    # returning data from a text file that contains json elements

    with open("static/events18.json", "r") as input_data:
        # you should use something else here than just plaintext
        # check out jsonfiy method or the built in json module
        # http://flask.pocoo.org/docs/0.10/api/#module-flask.json
        return input_data.read()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)