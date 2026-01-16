from django.views.defaults import page_not_found
from django.shortcuts import render_to_response
from django.template import RequestContext

def mi_error_404(request):
    return page_not_found(request,template_name='404.html')

def handler500(request,*args,**argv):
    response=render_to_response('500.html',{},context_instance=RequestContext(request))
    response.status_code=500
    return response 