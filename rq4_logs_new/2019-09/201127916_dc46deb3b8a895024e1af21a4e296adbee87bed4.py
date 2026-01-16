from flask import jsonify
from flask_restplus import Resource, fields, Namespace
from ...resources.order import AdminOrder, UserOrder
# FileStorage allows us to import files from http requests.
# from werkzeug.datastructures import FileStorage

from .user.get import Get as UserGet, Parser as user_order_add_parser
from .user.post import Post as UserPost, Parser as user_order_get_parser
from .user.put import Put as UserPut, Parser as user_order_update_parser
from .user.delete import Delete as UserDelete, Parser as user_order_delete_parser
from .admin.get import Get as AdminGet, Parser as admin_order_get_parser
from .admin.put import Put as AdminPut, Parser as admin_order_update_parser

user_ns = Namespace(
    "orders",
    description="Endpoints that allow customers to manage their orders")
admin_ns = Namespace(
    "admin/orders",
    description="Endpoints that allow admins to manage store orders")

#########################################################
#   User section
#########################################################


@user_ns.route("/")
class UserOrders(Resource):
    @user_ns.expect(user_order_get_parser)
    def get(self):
        '''
        Returns a sorted and filtered list of orders that the user has made
        '''
        args = user_order_get_parser.parse_args()

        return UserGet(args)

    @user_ns.response(200, 'Order succesfully added')
    @user_ns.response(403, 'Customer already has an order')
    @user_ns.expect(user_order_add_parser)
    def post(self):
        '''
        Creates new order "In Checkout"
        It should be called as soon as the checkout button is clicked to freeze the items in. 
        It gets the item-list from the user's token by grabbing the cart ID. Then using it to grab a list of product IDs, which then it recovers.
        It will save the items as complete objects at the time it is created.
        Assigns a unique "In Checkout" status to the order. An user may not create more orders if it has an "In Checkout" order. 
        If the user already has an In Checkout order the user should be prompted to delete the old one or to resume with this one. 
        '''
        args = user_order_add_parser.parse_args()

        return UserPost(args)

    @user_ns.response(403, "User does not have an order to update")
    @user_ns.expect(user_order_update_parser)
    def put(self):
        '''
        Finishes an user's order by adding in billing, shipping and payment information after it was reviewed. 
        This should also clear up the user's cart. 
        '''
        args = user_order_add_parser.parse_args()

        return UserPut(args)

    @user_ns.expect(user_order_delete_parser)
    def delete(self):
        '''
        Deletes the user's "In Checkout" order. This happens if the user backs-down from checkout. 
        '''
        args = user_order_get_parser.parse_args()
        return UserDelete(args)


#########################################################
#   Admin section
#########################################################


@admin_ns.route('/')
@admin_ns.response(403, "User is not an admin")
class AdminOrders(Resource):
    @admin_ns.response(200, 'Order successfully updated')
    @admin_ns.expect(admin_order_update_parser)
    def put(self):
        '''
        Updates a order status on the database
        '''
        args = admin_order_update_parser.parse_args()
        return AdminPut(args)

    @admin_ns.expect(admin_order_get_parser)
    def get(self):
        """
        Returns a list of orders from all the users. May be sorted and filtered.
        """
        args = admin_order_get_parser.parse_args()
        return AdminGet(args)