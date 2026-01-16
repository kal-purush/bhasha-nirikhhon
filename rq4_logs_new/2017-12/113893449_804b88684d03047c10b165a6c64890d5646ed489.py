
from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.shortcuts import render, HttpResponse
from django.http import JsonResponse

class ExceptionMiddleware:

    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        # Code to be executed for each request before
        # the view (and later middleware) are called.

        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        return response

    # Check if user is authenticated or not
    def process_exception(self, request, exception):
        return JsonResponse({'error_message':"Something Went Wrong",  'status':500})