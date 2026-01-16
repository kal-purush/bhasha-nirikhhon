# from rest_framework import status
from rest_framework.generics import CreateAPIView
from rest_framework.permissions import IsAuthenticated

# from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from tickets.models import Ticket
from tickets.serializers import TicketSerializer


class TicketAPIViewSet(ModelViewSet):
    queryset = Ticket.objects.all()
    serializer_class = TicketSerializer
    permission_classes = [IsAuthenticated]


class TicketCreateAPIView(CreateAPIView):
    pass


class TicketListAPIView(CreateAPIView):
    pass


class TicketRetrieveAPIView(CreateAPIView):
    pass


class TicketDeleteAPIView(CreateAPIView):
    pass


class TicketUpdateAPIView(CreateAPIView):
    pass