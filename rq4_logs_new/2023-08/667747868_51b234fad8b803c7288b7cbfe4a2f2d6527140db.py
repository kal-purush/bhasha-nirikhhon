from .serializers import UserSerializer
from django.utils.translation import gettext as _
from django.contrib.auth.models import User
from rest_framework.decorators import api_view
from django.contrib.auth import login
from rest_framework import status
from rest_framework.response import Response
from rest_framework_simplejwt.tokens import RefreshToken

@api_view(["POST"])
def login_view(request):
    username = request.data.get('username')
    password = request.data.get('password')
    try:
        user = User.objects.get(username=username, is_active=True)
    except User.DoesNotExist:
        return Response({"message": _("Account does not exist")}, status=status.HTTP_401_UNAUTHORIZED)

    if user.check_password(password):
        serializer = UserSerializer(user)
        login(request, user)
        refresh = RefreshToken.for_user(user)
        access_token = str(refresh.access_token)
        return Response({"user": serializer.data, "access_token": access_token, "message": _("Login successful"), "status": status.HTTP_200_OK})
    else:
        return Response({"message": _("Incorrect username or password")}, status=status.HTTP_401_UNAUTHORIZED)