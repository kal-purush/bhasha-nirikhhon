# your_app/middleware.py

from django.shortcuts import render
from django.utils.deprecation import MiddlewareMixin

class Custom404Middleware(MiddlewareMixin):
    def process_response(self, request, response):
        if response.status_code == 404:
            return render(request, '404.html', status=404)
        return response