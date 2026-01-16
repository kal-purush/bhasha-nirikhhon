from django_filters import FilterSet
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import viewsets

from core.models import OrderProduct
from core.serializers import OrderProductSerializer


class OrderProductFilterSet(FilterSet):
    class Meta:
        model = OrderProduct
        fields = {}


class OrderProductView(viewsets.ModelViewSet):
    queryset = OrderProduct.objects.all()
    serializer_class = OrderProductSerializer
    filter_backends = [DjangoFilterBackend]
    filter_class = OrderProductFilterSet