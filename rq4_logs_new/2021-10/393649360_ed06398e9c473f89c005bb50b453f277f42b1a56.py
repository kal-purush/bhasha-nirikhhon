from flask import Flask, request, Response


from validators import validate_and_save
from utils import get_flights               # New

app = Flask(__name__)


# 127.0.0.1:6000/add_flight
# New
@app.route('/add_flight', methods=['POST'])
def add_flight():
    status_code, errors = validate_and_save(json_object=request.json, object_type='flight')
    return Response(response=errors, status=status_code, mimetype='application/json')


# New
@app.route('/get_flights', methods=['GET'])
def get_flights():
    flights = get_flights()
    return Response(response=flights, mimetype='application/json')


@app.route('/add_ac', methods=['POST'])
def add_ac():
    status_code, errors = validate_and_save(json_object=request.json, object_type='ac')
    return Response(response=errors, status=status_code, mimetype='application/json')


if __name__ == '__main__':
    app.run(port=6000, debug=True)