from django.shortcuts import render
from django.http import JsonResponse
import logging

logger = logging.getLogger('console')

def GET(request):
    if request.method != 'GET' or 'model' not in request.GET or 'filter' not in request.GET:
        if 'model' not in request.GET:
            logger.error('API ERROR - GET: No model supplied in query parameters!')
        if 'filter' not in request.GET:
            logger.error('API ERROR - GET: No model filter supplied in query parameters!')
        
        return JsonResponse({}, status=400)
    
    model = request.GET['model'] 
    model_filter = request.GET['filter'] 

    try:
        model = {'model': {}}  # TODO - GET MODEL AND ADD TO JSON
        return JsonResponse(model, status=200)

    except Exception as e:
        logger.error('API ERROR - GET: {}'.format(str(e)))
        return JsonResponse({}, status=404)


    