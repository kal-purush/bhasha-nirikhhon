from django.conf.urls import include, url
from django.contrib import admin
from bucketlistapi import views
from bucketlistapp import views as app_view
from rest_framework.authtoken import views as rest_views
from django.contrib.auth.views import logout
import bucketlistapp.urls
import bucketlistapi.urls

urlpatterns = [
    url(r'^auth/', include('rest_framework.urls', namespace='rest_framework')),
    url(r'^api/users/$', views.UserList.as_view(), name='all_users'),
    url(r'^api/users/(?P<pk>[0-9]+)/$', views.UserDetail.as_view()),
    url(r'^api/bucketlists/$', views.BucketlistView.as_view(), name='bucket_list'),
    url(r'^api/bucketlists/(?P<pk>[0-9]+)/$', views.BucketListDetailView.as_view(), name='bucketlist_detail'),
    url(r'^api/bucketlists/(?P<pk>[0-9]+)/items/$', views.BucketlistItemView.as_view(), name='bucketlist_item'),
    url(r'^api/bucketlists/(?P<pk>[0-9]+)/items/(?P<id>[0-9]+)$', views.BucketListDetailView.as_view(), name='item_detail'),
    url(r'^api_token/$', rest_views.obtain_auth_token),
]