
import base64
import json
import logging
from rest_framework import authentication
from rest_framework import exceptions

logger = logging.getLogger(__name__)

class NamespacesHeaderAuthentication(authentication.BaseAuthentication):

    def authenticate(self, request):
        """ Authenticates if the x-namespace header is present, 
            and sets this as the auth namespaces for 
        """
        auth_data = {}
        if namespace:=request.headers.get("x-namespace"): 
            auth_data["namespaces"] = [namespace] 
            return (True, auth_data)
        else:
            msg = "x-namespace header not present on request."
            logger.debug(msg)
            raise exceptions.AuthenticationFailed(msg)