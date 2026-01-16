from django.contrib.auth.models import User
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import viewsets
from rest_framework.filters import SearchFilter
from rest_framework.permissions import IsAuthenticated

from persons.serializers import UserSerializer


class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = (IsAuthenticated,)
    filter_backends = [SearchFilter, DjangoFilterBackend]
    # search_fields = [
    #     "user__username", "user__first_name", "user__last_name", "user__email"
    # ]
    # filterset_fields = ["person__id",]