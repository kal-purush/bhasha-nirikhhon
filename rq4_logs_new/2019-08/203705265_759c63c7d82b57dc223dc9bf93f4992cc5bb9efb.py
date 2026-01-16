from django.urls import path
# 현재 디렉토리의 view를 가지고 오겠다.
from . import views

# articles/___
urlpatterns = [
    path('create/', views.create),
    path('new/', views.new),
    path('index/', views.index),
    
]