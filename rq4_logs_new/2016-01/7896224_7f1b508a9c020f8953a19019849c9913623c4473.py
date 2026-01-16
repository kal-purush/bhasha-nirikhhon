# -*- coding: utf-8 -*-
from django import http
from django.template import RequestContext, TemplateDoesNotExist, loader, Template
from django.utils.translation import get_language

# this method is for django version < 1.9
# for newer versions use view.v2.page_not_found

def page_not_found(request, template_name='404.html'):
    """
    Standard 404 view + feincms_page in context. Like this we can display
    navigations in 404 page, without using the add_page_if_missing middleware.
    We don't want to use the middleware, cause the page preview function,
    doe't work correctly with it.
    """
    try:
        template = loader.get_template(template_name)
        content_type = None             # Django will use DEFAULT_CONTENT_TYPE
    except TemplateDoesNotExist:
        template = Template(
            '<h1>Not Found</h1>'
            '<p>The requested URL {{ request_path }} was not found on this server.</p>')
        content_type = 'text/html'
    try:
        page = Page.objects.active().filter(language=get_language())[0]
    except IndexError:
        page = None
    request._feincms_page = page
    body = template.render(RequestContext(request, {'request_path': request.path, 'feincms_page': page}))
    return http.HttpResponseNotFound(body, content_type=content_type)