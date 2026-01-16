import time
from .models import Log


class LogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        # Code to be executed for each request before
        # the view (and later middleware) are called.
        t1 = time.time()
        response = self.get_response(request)
        t2 = time.time()
        if request.path == '/admin/':
            data = {
                'path': request.path,
                'method': request.method,
                'time': t2 - t1
            }
            log = Log(**data)
            log.save()

        # Code to be executed for each request/response after
        # the view is called.

        return response