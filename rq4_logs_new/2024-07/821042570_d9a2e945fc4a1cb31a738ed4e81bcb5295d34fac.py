from django_filters import rest_framework as filters
from .models import ProduccionDiaria

class ProduccionDiariaFilter(filters.FilterSet):
    estacion = filters.CharFilter(field_name='estacion', lookup_expr='iexact')

    class Meta:
        model = ProduccionDiaria
        fields = ['estacion','fecha',]