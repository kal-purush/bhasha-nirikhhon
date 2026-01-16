from flask import Flask, jsonify, request, redirect
import requests
app = Flask(__name__)

"""
Requesting from Data
"""
@app.route("/")
def index():
    return "<h1>Hello this app is to find the temperature of any location on earth<h1>"


@app.route('/theform', methods=['GET', 'POST'])
def theform():
    return '''<form method="POST" action="temperature">
                      <label for="fname">Latitude:</label>
                      <input type="text" name="latitude">
                      <label for="fname">Longitude:</label>
                      <input type="text" name="longitude">
                      <input type="submit" value="Submit">
                  </form>'''


@app.route('/temperature', methods=['POST'])
def process():
    latitude = request.form['latitude']
    longitude = request.form['longitude']
    r = requests.get('http://api.openweathermap.org/data/2.5/weather?lat='+latitude+'&lon='+longitude+'&appid=01b63eabf678d6725a9a05243d469b2b')
    json_object = r.json()
    temp_k = float(json_object['main']['temp'])
    temp_f = (temp_k - 273.15) * 1.8 + 32
    temp_c = int((temp_f - 32 ) * 5 / 9)
    Location = json_object['name']
    return '<h1>Hello  the {} temperature is {} Celsius <h1>'.format(Location, temp_c)


if __name__ == "__main__":
    app.run(debug=True,host="0.0.0.0")