from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps
import calendar
import json
import MySQLdb

app = Flask(__name__)
api = Api(app)

class Exchange_Address_From(Resource):
    def get(self, address_from):
        db = MySQLdb.connect(host="localhost", user="root", passwd="Sebis2017")
        cur = db.cursor()
        cur.execute("SELECT * FROM cross_block.exchanges WHERE address_from = %s", (address_from,))
        results = cur.fetchall()

        empList = []
        for emp in results:
            empDict = {
                "id": emp[0],
                "probability": "90%",
                "currency_from": emp[1],
                "currency_to": emp[2],
                "amount_from": str(emp[3]),
                "amount_to": str(emp[4]),
                "fee_from": str(emp[5]),
                "fee_to": str(emp[6]),
                "address_from": emp[8],
                "address_to": emp[9],
                "hash_from": emp[10],
                "hash_to": emp[11],
                "time_block_from": calendar.timegm(emp[13].timetuple()),
                "time_block_to": calendar.timegm(emp[15].timetuple()),
                "block_nr_from": emp[16],
                "block_nr_to": emp[17],
                "dollarvalue_from": emp[18],
                "dollarvalue_to": emp[19]
            }
            empList.append(empDict)

        print(json.dumps(empList))
        return json.dumps(empList)

        #return {'exchanges': [i[0] for i in results]}

api.add_resource(Exchange_Address_From, '/exchanges/<string:address_from>')

if __name__ == '__main__':
    app.run(debug=True)