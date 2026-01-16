from django.urls import path
from . import views

urlpatterns = [
    path('generate-ideas/', views.generate_ideas_view, name='generate_ideas')
]