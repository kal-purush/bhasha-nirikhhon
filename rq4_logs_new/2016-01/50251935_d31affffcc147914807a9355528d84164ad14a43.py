from django.conf.urls import patterns, include, url

urlpatterns = patterns('',
        (r'^$', lambda r: HttpResponseRedirect('myapp/'))
)