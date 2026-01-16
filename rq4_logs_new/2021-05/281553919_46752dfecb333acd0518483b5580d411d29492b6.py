

from uuid import uuid4

class Middleware():

    def __init__(self,app):
        self.app = app
        self.flask_header_name = "MD-REQ-ID"


    def __call__(self,environ,start_response):
        request_id_header = str(uuid4())
        environ[self.flask_header_name] = request_id_header

        def overwrite_start_response(status, response_headers):
            response_headers.append((self.flask_header_name, request_id_header))
            return start_response(status, response_headers)

        return self.app(environ, overwrite_start_response)

