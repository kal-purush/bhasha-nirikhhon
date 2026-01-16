from pyramid.view import (
    view_config,
    view_defaults
    )
from ..models.Item import item
import transaction


# @view_defaults(route_name='add_item_admin')
class add_item_view(object):
	param_for_item = {'name' : '',
					'main_category' : '',
					'sub_category' : '',
					'type_' : '',
					'storage' : '',
					'value' : 1,
					'subject_name' : '',
					'note' : ''}

	def __init__(self, request):
		self.request = request

	def create_item(self, **kwargs):
		with transaction.manager:
			session = self.request.dbsession
			itm = item()
			for key,value in self.param_for_item.items() :
				getattr(itm, i) = kwargs.get(key, value)

			session.add(itm)





