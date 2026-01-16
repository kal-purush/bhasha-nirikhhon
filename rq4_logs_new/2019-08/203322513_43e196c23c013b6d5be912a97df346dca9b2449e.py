
from django.http import JsonResponse
from django.conf import settings
import logging, json

def api_response(content = {}, code = 400):
    ''' Format an API JSON response.

        @content (dict) - A dictionary of JSON serializable objects to return.
        
        @code (int) - The HTTP status code to return. 
        
        Returns (JsonResponse): {<<content>>, code: <<code>> }
    '''

    content['code'] = code
    return JsonResponse(content, status=code)

def log_error(message):
    ''' Logs an API error to /logs/api.log if LOG_ERRORS is True in settings

        @message (string) - The message to log.
    '''

    if settings.DEBUG:
        logger = logging.getLogger('api')
        logger.error(message)

def handle_unsupported_method(request):
    if len(request.body) > 2:
        request.POST = json.loads(request.body.decode('utf-8').replace('\'', '\"'))

