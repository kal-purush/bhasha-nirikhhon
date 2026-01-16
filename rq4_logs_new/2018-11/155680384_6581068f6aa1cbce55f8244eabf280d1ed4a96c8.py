from flask import Flask, jsonify, Response,render_template
from flask import request
from pymongo import MongoClient
import datetime
import matplotlib.pyplot as plt
client = MongoClient('localhost', 27017)
mydb = client['ebslab']
database = mydb['ebslab']

app=Flask(__name__)

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r

@app.route('/')
def home():
    document = database.find().sort([('_id', -1)]).limit(1)
    for each in document:
        temperature = each["Temperature"]
        moisture = each["Moisture"]
        waterused = each["WaterUsed"]
        time = each["time"]
    plot()
    return render_template('index.html',temperature=str(temperature)+"C",moisture=str(moisture)+"%",
                           waterused=str(waterused)+"L", time=time)

def plot():
    a = database.find().sort([('_id', -1)]).limit(50)  # get the latest 50 records to plot
    temperature = []
    moisture = []
    waterused = []
    time = []
    for each in a:
        temperature.append(each["Temperature"])
        moisture.append(each["Moisture"])
        waterused.append(each["WaterUsed"])
        time.append(each["time"])
    (fig, ax) = plt.subplots(1, 1)
    ax.plot(time, temperature)
    ax.set_xlabel("Date and Time", fontsize=12)
    ax.set_ylabel("Temperature", fontsize=12)
    fig.savefig("./static/images/temperature.jpg")
    (fig, ax) = plt.subplots(1, 1)
    ax.plot(time, moisture)
    ax.set_xlabel("Date and Time", fontsize=12)
    ax.set_ylabel("Moisture", fontsize=12)
    fig.savefig("./static/images/moisture.jpg")
    (fig, ax) = plt.subplots(1, 1)
    ax.plot(time, waterused)
    ax.set_xlabel("Date and Time", fontsize=12)
    ax.set_ylabel("waterused", fontsize=12)
    fig.savefig("./static/images/waterused.jpg")

@app.route('/data/', methods=['POST'])
def data():
    data=request.get_json()
    time = datetime.datetime.now()
    data["time"] = time
    print(data)
    database.insert_one(data)
    return "success"



if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=8000)