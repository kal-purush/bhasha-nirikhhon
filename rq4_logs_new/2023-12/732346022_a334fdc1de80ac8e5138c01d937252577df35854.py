from django.urls import path
from .views import AnimalsView


urlpatterns = [
    path('animals/', AnimalsView.as_view(), name='animals')
]