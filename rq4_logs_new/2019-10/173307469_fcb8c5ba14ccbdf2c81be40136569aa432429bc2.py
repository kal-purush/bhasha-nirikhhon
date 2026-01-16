import logging

import falcon

from am import FileController
from common.consts import HTTP_READ_METHODS, HTTP_IGNORE_METHODS, HTTP_WRITE_METHODS
from common.entities import UserAccessMeta


class RequireAuthGroups:
    def __init__(self, controller: FileController):
        self._controller = controller

    # the resource can configure that they perform the group validation internally
    def process_resource(self, req: falcon.Request, _resp: falcon.Response, resource, params):
        if getattr(resource, "internal_group_validation", False):
            return

        access = getattr(req, "user_access", UserAccessMeta())
        store_id = params.get("store_id", None)
        project_id = params.get("project_id", None)

        method = req.method

        if store_id and project_id:
            if method in HTTP_IGNORE_METHODS:
                return

            if method in HTTP_READ_METHODS and not self._controller.has_access(store_id=store_id, project_id=project_id,
                                                                               groups=access.read_groups, is_admin=access.admin_access):
                raise falcon.HTTPUnauthorized(title='Access token required for READ operation',
                                              description='Please provide a valid access token as part of the request.')
            elif method in HTTP_WRITE_METHODS and not self._controller.has_access(store_id=store_id, project_id=project_id,
                                                                                  groups=access.write_groups, is_admin=access.admin_access):
                raise falcon.HTTPUnauthorized(title='Access token required for WRITE operation',
                                              description='Please provide a valid access token as part of the request.')
        else:
            logging.error("Something is wrong with this resource %s. It should either declare itself as 'internal_group_validation' "
                          "or have a valid store_id and project_id.", req.path)
            raise falcon.HTTPInternalServerError(title="Auth service configuration error", description="One of the resource is misconfigured")