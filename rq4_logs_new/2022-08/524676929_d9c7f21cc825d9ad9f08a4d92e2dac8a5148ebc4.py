from flask import Flask, request, jsonify
from model import SensorModel
from db import db_create_sensor, db_list_sensors

app = Flask(__name__)

### Routing dan Method CRUD

## "CREATE" data/post data ke database
@app.route("/create", methods=["POST"])
def create_sensor():
    """"
    Create a new sensor
    """
    data = request.get_json()
    status = SensorModel.Schema().validate(data)
    if status:
        return jsonify(status), 404
    sensor = SensorModel.from_dict(data)
    db_create_sensor(sensor)
    return jsonify(data=sensor.to_dict()), 200

## "GET" ambil data dari database
@app.route("/list", methods=["GET"])
def list_sensor():
    """"
    Retrieve all sensors
    """
    # Find/get all data di sensor disimpan di var sensor
    sensors = db_list_sensors()
    res = {"data-all-sensor": [sensor.to_dict() for sensor in sensors], "count": len(sensors)}
    return jsonify(data=res), 200

if __name__ == "__main__":
    app.run(debug=True)
