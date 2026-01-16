from django.conf import settings as django_settings

from debug_toolbar.toolbar import DebugToolbar

from security.config import settings

from .models import DebugToolbarData


class DebugToolbarLogMiddleware:

    def __init__(self, get_response=None):
        self.get_response = get_response
        super().__init__()

    def __call__(self, request):
        if settings.DEBUG_TOOLBAR and django_settings.DEBUG:
            toolbar = DebugToolbar(request, self.get_response)

            # Activate instrumentation ie. monkey-patch.
            for panel in toolbar.enabled_panels:
                panel.enable_instrumentation()
            try:
                # Run panels like Django middleware.
                response = toolbar.process_request(request)
            finally:
                # Deactivate instrumentation ie. monkey-unpatch. This must run
                # regardless of the response. Keep 'return' clauses below.
                for panel in reversed(toolbar.enabled_panels):
                    panel.disable_instrumentation()

            for panel in reversed(toolbar.enabled_panels):
                panel.generate_stats(request, response)
                panel.generate_server_timing(request, response)

            if getattr(request, 'input_logged_request', False):

                DebugToolbarData.objects.create(
                    logged_request=request.input_logged_request, toolbar=toolbar.render_toolbar()
                )
            return response
        else:
            return self.get_response(request)