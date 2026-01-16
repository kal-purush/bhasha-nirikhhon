from django.urls import path
from .views import *


urlpatterns = [
    path('poll/',polls_page,name='polls'),
    path('question/<int:poll_id>/',questions_page,name='question'),
]