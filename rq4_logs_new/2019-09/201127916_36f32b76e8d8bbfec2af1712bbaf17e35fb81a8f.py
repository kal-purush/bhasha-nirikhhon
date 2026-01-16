from flask import jsonify
from flask_restplus import Resource, fields, Namespace

from .customer.post import Post as CustomerPost, Parser as customer_login_parser
from .customer.delete import Delete as CustomerDelete, Parser as customer_logout_parser
from .admin.post import Post as AdminPost, Parser as admin_login_parser
from .admin.delete import Delete as AdminDelete, Parser as admin_logout_parser

customer_ns = Namespace(
    "session",
    description="Endpoints that allow customers to interact with their session"
)

admin_ns = Namespace(
    "admin/session",
    description="Endpoints that allow admins to interact with their session")


@customer_ns.route("/")
@customer_ns.response(401, "Invalid Credentials")
class CustomerSession(Resource):
    @customer_ns.expect(customer_login_parser)
    def post(self):
        '''
        Allows the customer to log in with their email and password
        '''
        args = customer_login_parser.parse_args()
        return CustomerPost(args)

    @customer_ns.expect(customer_logout_parser)
    def delete(self):
        '''
        Allows the customer to log out of their current session
        '''
        args = customer_logout_parser.parse_args()
        return CustomerDelete(args)


@admin_ns.route("/")
@admin_ns.response(401, "Invalid Credentials")
class AdminSession(Resource):
    @admin_ns.expect(admin_login_parser)
    def post(self):
        '''
        Allows the admin to log in with their email and password
        '''
        args = admin_login_parser.parse_args()
        return AdminPost(args)

    @admin_ns.expect(admin_logout_parser)
    def delete(self):
        '''
        Allows the admin to log out of their current session
        '''
        args = admin_logout_parser.parse_args()
        return AdminDelete(args)