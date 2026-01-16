from flask_restplus import Api, Resource, abort, marshal

from . import API
from .api_models import BEVERAGE_GET

from ..db_models.beverage import Beverage

BEVERAGE_NS = API.namespace('beverages', description='Beverages', path='/beverages')

@BEVERAGE_NS.route('/')
class BeverageList(Resource):
    """
    Beverages
    """

    #@jwt_required
    #@API.param('active', 'get only active lendings', type=bool, required=False, default=True)
    @API.marshal_list_with(BEVERAGE_GET)
    # pylint: disable=R0201
    def get(self):
        """
        Get a list of all lendings currently in the system
        """
        #active = request.args.get('active', 'false') == 'true'

        #if active:
        #    base_query = base_query.join(ItemToLending).distinct()
        return Beverage.query.all()