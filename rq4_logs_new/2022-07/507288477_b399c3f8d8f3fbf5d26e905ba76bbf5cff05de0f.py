
from rest_framework.serializers import ModelSerializer

from src.dealership.models import Dealership, Location, ProviderCars


class LocationSerializer(ModelSerializer):

    class Meta:
        model = Location
        fields = ('__all__')


class DealershipSerializer(ModelSerializer):

    class Meta:
        model = Dealership
        fields = ('name', 'location', 'description', 'balance', 'cars', )


class ProviderCarsSerializer(ModelSerializer):

    class Meta:
        model = ProviderCars
        fields = ('count', 'dealership', 'cars', 'provider', )