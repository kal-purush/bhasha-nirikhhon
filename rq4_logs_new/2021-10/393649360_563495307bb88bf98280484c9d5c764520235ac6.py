from datetime import datetime
from flask import Flask
from flask_restful import Api, Resource, marshal_with

from validator import flight_parser, flight_template

app = Flask(__name__)
api = Api(app)


test_data = {
    "_id": 45023,
    "name": "123",
    "email": "max@mail.com",
    "password": "12DFRTI34",
    "dob": datetime.now()
}


class Flight(Resource):
    @marshal_with(flight_template)
    def get(self):
        return test_data

    def post(self):
        args = flight_parser.parse_args()
        return args


api.add_resource(Flight, "/flight")


if __name__ == "__main__":
    app.run(debug=True)