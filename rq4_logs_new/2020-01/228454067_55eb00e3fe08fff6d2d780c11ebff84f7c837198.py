from logs import Logger
from entities.response import Response
import flask


def http_response(func):

    def new_function(*args, **kwargs):
        logger = Logger('http_repsonse::helpers::flask')
        response = func(*args, **kwargs)

        if not isinstance(response, Response):
            logger.error('The controler does not return a valid response')
            raise Exception

        if not callable(getattr(response.body, 'to_json', None)):
            logger.error('The body response is not serializable to json')
            raise Exception

        web_response = flask.Response(
            response.body.to_json(),
            status=response.status
        )
        web_response.headers['Content-Type'] = 'application/json'

        return web_response

    return new_function