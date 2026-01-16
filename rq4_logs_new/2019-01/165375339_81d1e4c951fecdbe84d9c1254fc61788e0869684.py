from datetime import datetime
from flask import request
from flask_restplus import Api, Resource, abort, marshal
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func

from . import API
from . import APP
from .api_models import TRANSACTION_PUT
from .api_models import TRANSACTION_GET
from .api_models import TRANSACTION_POST
from .api_models import TRANSACTION_DELETE

from .. import DB
from ..db_models.transaction import Transaction
from ..db_models.transaction_beverage import TransactionBeverage

USER_NS = API.namespace('users', description='Users', path='/users')

@USER_NS.route('/<user_id>/transactions')
class BeverageList(Resource):
    """
    List of all Transactions
    """

    #@jwt_required
    #@API.param('active', 'get only active lendings', type=bool, required=False, default=True)
    @API.marshal_list_with(TRANSACTION_GET)
    # pylint: disable=R0201
    def get(self, user_id: int):
        """
        Get a list of all transactions of the specified user currently in the system
        """
        return Transaction.query.filter(Transaction.user_id == user_id).all()

    #@jwt_required
    #@satisfies_role(UserRole.ADMIN)
    @USER_NS.doc(model=TRANSACTION_GET, body=TRANSACTION_POST)
    @USER_NS.response(409, 'Name is not unique!')
    @USER_NS.response(201, 'Created.')
    # pylint: disable=R0201
    def post(self):
        """
        Add a new transaction to the database
        """
        new_transaction = Transaction(**request.get_json())
        beverages = Transaction(**request.get_json()['beverages'])
        try:
            DB.session.add(new_transaction)
            for beverage in beverages:
                new_beverage = TransactionBeverage(new_transaction, **beverage)
                DB.session.add(new_beverage)
            DB.session.commit()
            return marshal(new_transaction, TRANSACTION_GET), 201
        except IntegrityError as err:
            message = str(err)
            if APP.config['DB_UNIQUE_CONSTRAIN_FAIL'] in message:
                abort(409, 'Name is not unique!')
            abort(500)


@USER_NS.route('/<int:user_id>/transactions/<int:transaction_id>')
class UserDetail(Resource):
    """
    A single transaction
    """

    #@jwt_required
    @API.marshal_with(TRANSACTION_GET)
    # pylint: disable=R0201
    def get(self, transaction_id):
        """
        Get the details of a single transaction
        """
        return Transaction.query.filter(Transaction.id == transaction_id).first()

    #@jwt_required
    #@satisfies_role(UserRole.ADMIN)
    @USER_NS.doc(model=TRANSACTION_GET, body=TRANSACTION_PUT)
    @USER_NS.response(404, 'Requested transaction not found!')
    # pylint: disable=R0201
    def put(self, transaction_id):
        """
        Update a single transaction
        """
        transaction = Transaction.query.filter(Transaction.id == transaction_id).first()

        if transaction is None:
            abort(404, 'Requested transaction not found!')

        transaction.update(**request.get_json())
        DB.session.commit()
        return marshal(transaction, TRANSACTION_GET), 200

    #@jwt_required
    #@satisfies_role(UserRole.ADMIN)
    @USER_NS.doc(model=TRANSACTION_GET, body=TRANSACTION_DELETE)
    @USER_NS.response(410, 'Reverse Deadline not met')
    # pylint: disable=R0201
    def delete(self, transaction_id):
        """
        Revert specified transaction in the database (adds a revertTransaction)
        """
        reason = request.get_json()['reason']
        transaction = Transaction.query.filter(Transaction.id == transaction_id).first()
        #TODO timestamp bei >func.now()+5 min
        if transaction.timestamp > func.now()+ datetime.minute(5):
            abort(410, 'Reverse Deadline not met')
        else:
            reverse_transaction = Transaction(transaction.user, -transaction.amount, reason, transaction)
            beverages = transaction.beverages
            try:
                DB.session.add(reverse_transaction)
                for beverage in beverages:
                    reverse_beverage = TransactionBeverage(reverse_transaction, beverage.beverage, -beverage.count, beverage.price)
                    DB.session.add(reverse_beverage)
                DB.session.commit()
                return marshal(reverse_transaction, TRANSACTION_GET), 201
            except IntegrityError as err:
                message = str(err)
                if APP.config['DB_UNIQUE_CONSTRAIN_FAIL'] in message:
                    abort(409, 'Name is not unique!')
                abort(500)