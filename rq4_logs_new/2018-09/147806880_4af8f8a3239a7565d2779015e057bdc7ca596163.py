from flask import Flask
from flask import request
from flask_restful import Resource, Api, reqparse
from flask_jwt import JWT, jwt_required

# local import
from auth import auth, identity

# Our App
app = Flask(__name__)
app.secret_key = 'thisismysecretkey'
api = Api(app)

# Initialize JWT
jwt = JWT(app, auth, identity)

orders = []


class Order(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('price',
        type=float,
        required=True,
        help="Please fill in this field!"
    )

    # Let us authenticate first before we go to the GET request
    @jwt_required()
    def get(self, name):
        order = next(filter(lambda x: x['name'] == name, orders), None)
        return {'order': order}, 200 if order else 404

    def post(self, name):
        # Deal with the errors first
        if next(filter(lambda x: x['name'] == name, orders), None):
            return {'message': "An order with the name '{}' already exists.".format(name)}, 400

        # load data
        data = Order.parser.parse_args()

        order = {'name': name, 'price': data['price']}
        orders.append(order)
        return order, 201
    
    def delete(self, name):
        global orders
        orders = list(filter(lambda x: x['name'] != name, orders))
        return {'message': 'Order deleted'}

    def put(self, name):
        data = Order.parser.parse_args()

        order = next(filter(lambda x: x['name'] == name, orders), None)
        if order is None:
            order = {'name': name, 'price': data['price']}
            orders.append(order)
        else:
            order.update(data)
        return order


class OrderList(Resource):
    def get(self):
        return {'orders': orders}


api.add_resource(Order, '/v1/orders/<string:name>')
api.add_resource(OrderList, '/v1/orders')


if __name__ == '__main__':
    app.run(debug=True)