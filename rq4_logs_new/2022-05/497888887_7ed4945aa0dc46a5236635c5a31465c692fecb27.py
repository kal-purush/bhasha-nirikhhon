from django.urls import path
from accounts.views import RegistrationView

app_name= 'accounts'

urlpatterns = [
    path('register/', RegistrationView.as_view(), name='register')
]