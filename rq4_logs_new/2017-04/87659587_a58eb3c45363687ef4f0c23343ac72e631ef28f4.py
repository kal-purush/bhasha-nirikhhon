from . import views
from django.conf.urls import include, url

urlpatterns = [
    url(r'^$', views.post_list, name='post_list'),
]