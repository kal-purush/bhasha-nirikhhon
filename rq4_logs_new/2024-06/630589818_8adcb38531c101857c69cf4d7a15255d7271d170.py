from django.contrib import admin
from django.urls import path

from . import views


urlpatterns = [
    path('records/', views.records, name='records'),
    path('prescriptions', views.prescriptions),
]